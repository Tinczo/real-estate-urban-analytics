#!/bin/bash
# setup_nominatim.sh - uruchamia lokalny kontener Nominatim do geocodingu
# Uzycie: ./setup_nominatim.sh [pbf_path] [port]
# Przyklad: ./setup_nominatim.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PBF_PATH="${1:-$SCRIPT_DIR/../data/planet_20.762,52.139_21.267,52.334.osm.pbf}"
PORT="${2:-8083}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[ -f "$PBF_PATH" ] || error "Nie znaleziono pliku PBF: $PBF_PATH"

info "Zatrzymuje stary kontener nominatim (jesli istnieje)..."
docker rm -f nominatim 2>/dev/null || true

info "Uruchamiam Nominatim z pliku: $PBF_PATH (port: $PORT)"

docker run -d \
    -e PBF_PATH=/nominatim/data/warszawa.osm.pbf \
    -v "${PBF_PATH}:/nominatim/data/warszawa.osm.pbf" \
    -p "${PORT}:8080" \
    --name nominatim \
    mediagis/nominatim:5.3

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
        exit 1
    fi
    echo "  ... czekam na import ($attempts/60, co 10s)"
    sleep 10
done
