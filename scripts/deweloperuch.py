import requests
from pathlib import Path
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
import threading
import itertools
import logging
import shutil
import os
from datetime import datetime
import json
from config import DATA_DIR, PER_PAGE, RE_DIR, API_URL, MAX_THREADS, SOURCES, LOGS_DIR
from utils import unzip_file, flatten_and_keep_csv

thread_local = threading.local()
worker_counter = itertools.count(1) 
metadata_lock = threading.Lock()

log_filename = datetime.now().strftime("acquisition_%Y-%m-%d_%H-%M-%S.log")
log_path = LOGS_DIR / log_filename
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_meta_http(url):
    """Strategia HTTP dla OSM"""
    try:
        r = requests.head(url, allow_redirects=True, timeout=20)
        logger.info(f"Metadane OSM Poland | ETag: {r.headers.get('ETag')} | Size: {r.headers.get('Content-Length')}")
        return {"etag": r.headers.get("ETag"), "size": r.headers.get("Content-Length")}
    except Exception as e:
        logging.error(f"Błąd HTTP meta: {e}")
        return None

def get_meta_ftp(host):
    """Logika dla GTFS z dynamicznymi nazwami plików"""
    try:
        with FTP(host) as ftp:
            ftp.login()

            files = ftp.nlst()
            zip_files = [f for f in files if f.endswith('.zip')]
            
            if not zip_files:
                logging.error("Brak plików ZIP na serwerze FTP.")
                return None
            
            latest_file = sorted(zip_files)[-1]
            logging.info(f"Najnowszy plik na FTP to: {latest_file}")
            
            return {
                "filename": latest_file,
            }
    except Exception as e:
        logging.error(f"Błąd FTP (dynamiczna lista): {e}")
        return None
    
def download_ftp_file(host, remote_filename, local_path, logger):
    """Pobieranie pliku bezpośrednio przez protokół FTP"""
    try:
        bytes_downloaded = 0
        
        def file_write_with_counter(data):
            nonlocal bytes_downloaded
            bytes_downloaded += len(data)
            f.write(data)

        with FTP(host) as ftp:
            ftp.login()
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f"RETR {remote_filename}", file_write_with_counter, blocksize=1024*1024)
                
        mb_downloaded = bytes_downloaded / (1024 * 1024)
        logger.info(f"Pobrano plik FTP: {mb_downloaded:.2f} MB ({remote_filename})")
        return True
    except Exception as e:
        logger.error(f"Błąd pobierania FTP ({remote_filename}): {e}")
        return False

def download_http_file(url, local_path, logger, session=None):
    """Pobieranie pliku przez HTTP (dla OSM)"""
    requester = session if session is not None else requests
    try:
        with requester.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    
        mb_downloaded = int(r.headers.get("Content-Length",0)) / (1024 * 1024)
        logger.info(f"Pobrano plik: {mb_downloaded:.2f} MB z {url}")
        return True
    except Exception as e:
        logger.error(f"Błąd pobierania HTTP({url}): {e}")
        return False
    
def load_all_metadata():
    metadata_path = DATA_DIR / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Błąd odczytu metadanych: {e}. Tworzę nową bazę.")
    return {}

def save_all_metadata(metadata):
    metadata_path = DATA_DIR / "metadata.json"
    try:
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
    except Exception as e:
        logger.error(f"Błąd zapisu metadanych: {e}")
    
def sync_resource(name, config, dest_folder, metadata_all):
    """Główna, uniwersalna funkcja akwizycji"""
    logger.info(f"Sprawdzanie źródła: {name}")
    success = False
    save_path = dest_folder / f"{name}_latest.zip"
    
    # Pobierz nowe metadane
    if config["method"] == "http":
        remote_meta = get_meta_http(config["url"])
    else:
        remote_meta = get_meta_ftp(config["url"])

    if not remote_meta:
        return

    # Porównaj z lokalnymi
    local_meta = metadata_all.get(name)
    if local_meta == remote_meta:
        logger.info(f"Brak zmian dla {name}. Pomijam.")
        return
    
    logger.info(f"Wykryto nową wersję zasobu [{name}]. Rozpoczynam aktualizację lokalnych plików...")
    if config["method"] == "http":
        success = download_http_file(config["url"], save_path, logger)
    else:
        success = download_ftp_file(
            config["url"], 
            remote_meta['filename'], 
            save_path, 
            logger
        )

    if not success:
        return

    # Rozpakowanie
    target_dir = dest_folder / name
    
    if target_dir.exists():
        shutil.rmtree(target_dir) 
        
    success = unzip_file(save_path, target_dir)
    
    if not success:
        logger.error(f"Przerwanie aktualizacji {name} z powodu błędu ZIP.")
        if save_path.exists(): os.remove(save_path)
        return

    # Aktualizacja metadanych
    os.remove(save_path)
    metadata_all[name] = remote_meta
    save_all_metadata(metadata_all)
    logger.info(f"Zakończono aktualizację {name}.")

def get_worker_id():
    """Przydziela unikalny identyfikator (1-10) każdemu wątkowi."""
    if not hasattr(thread_local, "worker_id"):
        thread_local.worker_id = next(worker_counter)
    return thread_local.worker_id

def get_session():
    """Sesja dla watku"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def get_all_items(session, base_url, params=None, debug=False):
    current_url = base_url
    active_params = params

    while current_url:
        try:
            response = session.get(current_url, params=active_params, timeout=20)
            logger.info(f"GET {response.url} -> {response.status_code}")
            
            if response.status_code == 429 or response.status_code == 403:
                time.sleep(2)
                continue
                
            if response.status_code != 200:
                break
                
            json_data = response.json()
            data = json_data.get('data', [])
            
            if not data:
                break
                
            yield from data
            
            links = json_data.get('links', {})
            current_url = links.get('next')
            active_params = None
            
        except Exception as e:
            break


def process_dataset(dataset, metadata):
    session = get_session()
    worker_id = get_worker_id()
    dataset_link = dataset['links']['self']
    data = None

    try:
        data = session.get(dataset_link, timeout=20)
        data.raise_for_status()
        data =  data.json().get("data")
    except Exception as e:
        logger.warning(f"[Worker {worker_id:02d}] Błąd pobierania pełnego datasetu: {e}")
        return None

    id = data["id"]
    attr = data['attributes']
    download_url = attr['archived_resources_files_url']
    modified = attr['modified']

    meta = metadata.get(id, {})
    if not download_url or meta.get("modified") == modified:
        logger.info(f"Dataset {id} aktualny")
        return
    
    
    local_path = RE_DIR / id
    zip_path = local_path.with_suffix(".zip")

    if local_path.exists():
        shutil.rmtree(local_path)
        logger.info(f"[Worker {worker_id:02d}] Usunięto stary katalog: {local_path}")

    try:
        head = session.head(download_url, timeout=10)
        content_length = head.headers.get("content-length")
        download_http_file(download_url, zip_path, logger, session=session)
        logger.info(f"[Worker {worker_id:02d}] Zakończono pobieranie: {download_url}, "
                    f"Pobrano {int(content_length or 0) / (1024 * 1024):.2f} MB")

        
        if unzip_file(zip_path, local_path):
            flatten_and_keep_csv(local_path)
            zip_path.unlink()
            logger.info(f"[Worker {worker_id:02d}] Rozpakowano i wyczyszczono: {local_path}")

        with metadata_lock:
            metadata[id] = {"modified": modified, "content_length": content_length}
            save_all_metadata(metadata)
    except Exception as e:
        logger.warning(f"Nie udało się pobrać: {download_url} – {e}")

def developer_scanner(metadata_all):
    logger.info("=== URUCHAMIANIE SKANERA DEWELOPERÓW (MULTI-THREAD) ===")
    dataset_url = f"{API_URL}/datasets"
    dataset_params = {"keyword[contains]": "Deweloper", "per_page": PER_PAGE}

    with requests.Session() as main_session:
        datasets = get_all_items(main_session, dataset_url, dataset_params)

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for result in executor.map(
                lambda d: process_dataset(d, metadata_all), 
                datasets
            ):
                pass

    logger.info("=== SKANOWANIE ZAKOŃCZONE ===")

def main():
    metadata_all = load_all_metadata()
    for source_name, source_config in SOURCES.items():
        try:
            sync_resource(
                name=source_name, 
                config=source_config, 
                dest_folder=DATA_DIR, 
                metadata_all=metadata_all,
            )
        except Exception as e:
            logger.error(f"Nieoczekiwany błąd przy {source_name}: {e}", exc_info=True)

    developer_scanner(metadata_all)
    logger.info("=== Akwizycja zakończona ===")

if __name__ == "__main__":
    main()