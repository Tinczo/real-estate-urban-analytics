import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import concurrent.futures
import time
import threading
import itertools

API_URL = "https://api.dane.gov.pl/1.4"
PER_PAGE = 100
MAX_THREADS = 20
DESTINATION_FOLDER = "dane"

thread_local = threading.local()
worker_counter = itertools.count(1) 

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
            
            if response.status_code == 429:
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

def process_dataset(dataset):
    attr = dataset['attributes']
    slug = attr['slug'][:90] 
    resources_link = dataset['relationships']['resources']['links']['related']
    resources_params = {"per_page": PER_PAGE}

    session = get_session()
    worker_id = get_worker_id()
    
    desc_text = f"Wątek {worker_id:02d} | {slug}"
    
    with tqdm(desc=desc_text, position=worker_id, leave=False, bar_format="{desc} | Pobrane pliki: {n_fmt}") as w_pbar:
        for resource in get_all_items(session, resources_link, resources_params):
            files = resource['attributes'].get('files', [])
            if not files:
                continue
                
            file_info = files[0]
            download_url = file_info['download_url']
            file_format = file_info['format']
            
            file_id = download_url.split('/')[-2][:30]
            save_name = f"{file_id}.{file_format}"
            
            target_dir = Path(f"{DESTINATION_FOLDER}/{slug}")
            
            try:
                r = session.get(download_url, timeout=30, stream=True)
                if r.status_code == 200:
                    target_dir.mkdir(parents=True, exist_ok=True)
                    save_path = target_dir / save_name
                    
                    with open(save_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                    
                    w_pbar.update(1) 
            except Exception as e:
                pass

def main():
    print("=== URUCHAMIANIE SKANERA DEWELOPERUCHÓW (MULTI-THREAD) ===")
    dataset_url = f"{API_URL}/datasets"
    dataset_params = {"keyword[contains]": "Deweloper", "per_page": PER_PAGE}

    print("Pobieram listę datasetow...")
    with requests.Session() as main_session:
        datasets = list(get_all_items(main_session, dataset_url, dataset_params))
        
    print(f"Znaleziono {len(datasets)} datasetow. Start w {MAX_THREADS} wątkach.\n")
    
    print("\n" * MAX_THREADS)

    total = len(datasets)
    with tqdm(total=total, desc="OGÓLNY POSTĘP", position=0, leave=True) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(process_dataset, dataset) for dataset in datasets]
            for _ in concurrent.futures.as_completed(futures):
                pbar.update(1)

    print("\n\n=== GOTOWE ===")

if __name__ == "__main__":
    main()