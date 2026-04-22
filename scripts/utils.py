import zipfile
from pathlib import Path

def unzip_file(zip_path, extract_to):
    """
    Rozpakowuje plik ZIP do wskazanego folderu.
    zip_path: Path do pliku .zip
    extract_to: Path do folderu docelowego
    """
    try:
        if not zip_path.exists() or zip_path.stat().st_size == 0:
            return False

        extract_to.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            bad_file = zip_ref.testzip()
            if bad_file:
                return False
                
            zip_ref.extractall(extract_to)
            
        return True

    except zipfile.BadZipFile:
        pass
    except Exception as e:
        pass
    
    return False

def flatten_and_keep_csv(directory):
    """
    Spłaszcza strukturę katalogów – przenosi wszystkie CSV do katalogu głównego,
    usuwa pozostałe pliki i puste podkatalogi.
    """
    directory = Path(directory)
    
    csv_files = list(directory.rglob("*.csv"))
    
    for csv_path in csv_files:
        if csv_path.parent == directory:
            continue 
        
        target = directory / csv_path.name
        if target.exists():
            target = directory / f"{csv_path.stem}__{csv_path.parent.name}{csv_path.suffix}"
        
        csv_path.rename(target)
    
    for file_path in directory.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() != ".csv":
            file_path.unlink()
    
    for dir_path in sorted(directory.rglob("*"), reverse=True):
        if dir_path.is_dir():
            try:
                dir_path.rmdir()
            except OSError:
                pass