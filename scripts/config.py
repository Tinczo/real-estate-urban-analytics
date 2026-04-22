from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
RE_DIR = DATA_DIR / "real_estate"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
RE_DIR.mkdir(parents=True, exist_ok=True)

API_URL = "https://api.dane.gov.pl/1.4"
PER_PAGE = 100
MAX_THREADS = 20

SOURCES = {
    "gtfs": {
        "url": "gtfs.ztm.waw.pl",
        "method": "ftp"
    },
    "osm": {
        "url": "https://download3.bbbike.org/osm/parquet/region/europe/poland/poland.osm.parquet.zip",
        "method": "http"
    }
}