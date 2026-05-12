#!/bin/bash
# setup_nominatim.sh - uruchamia lokalny kontener Nominatim do geocodingu
# Uzycie: ./setup_nominatim.sh [pbf_path] [port]
# Przyklad: ./setup_nominatim.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PBF_PATH="${1:-$SCRIPT_DIR/../data/warsaw.osm.pbf}"
PORT="${2:-8083}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; GRAY='\033[1;30m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

if [ ! -f "$PBF_PATH" ]; then
    err "Nie znaleziono pliku PBF: $PBF_PATH"
    err "Pobierz plik OSM PBF i umiesz go w data/ lub podaj sciezke przez parametr"
    exit 1
fi
PBF_PATH="$(realpath "$PBF_PATH")"

info "Zatrzymuje stary kontener nominatim (jesli istnieje)..."
docker rm -f nominatim >/dev/null 2>&1 || true

info "Uruchamiam Nominatim z pliku: $PBF_PATH"
info "Port: $PORT"

docker run -d \
    -e PBF_PATH=/nominatim/data/warszawa.osm.pbf \
    -v "${PBF_PATH}:/nominatim/data/warszawa.osm.pbf" \
    -p "${PORT}:8080" \
    --name nominatim \
    mediagis/nominatim:5.3 >/dev/null

if [ $? -ne 0 ]; then
    err "Nie udalo sie uruchomic kontenera Nominatim."
    exit 1
fi

info "Kontener uruchomiony. Czekam az Nominatim bedzie gotowy..."

attempts=0
while true; do
    if curl -sf "http://localhost:${PORT}/search?q=Warszawa&format=json&limit=1" >/dev/null 2>&1; then
        info "Nominatim gotowy! http://localhost:${PORT}/search"
        break
    fi
    attempts=$((attempts + 1))
    if [ $attempts -gt 60 ]; then
        warn "Nominatim nie odpowiedzial po 10 minutach. Sprawdz: docker logs nominatim"
        warn "Moze trwac dluzej przy pierwszym imporcie danych."
        exit 1
    fi
    echo -e "${GRAY}  ... czekam na import ($attempts/60, co 10s)${NC}"
    sleep 10
done
