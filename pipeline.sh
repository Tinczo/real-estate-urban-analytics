#!/bin/bash
# pipeline.sh - master skrypt: stawia infrastrukture, przetwarza dane, uruchamia potok MapReduce
# Uzycie: ./pipeline.sh [--skip-infrastructure] [--skip-acquisition] [--skip-processing] [--skip-mapreduce] [--skip-experiments]
# Przyklad: ./pipeline.sh
# Przyklad (tylko przetwarzanie): ./pipeline.sh --skip-infrastructure --skip-acquisition
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR"
DATA="$ROOT/data"

SKIP_INFRASTRUCTURE=0
SKIP_ACQUISITION=0
SKIP_PROCESSING=0
SKIP_MAPREDUCE=0
SKIP_EXPERIMENTS=0

for arg in "$@"; do
    case "$arg" in
        --skip-infrastructure) SKIP_INFRASTRUCTURE=1 ;;
        --skip-acquisition)    SKIP_ACQUISITION=1 ;;
        --skip-processing)     SKIP_PROCESSING=1 ;;
        --skip-mapreduce)      SKIP_MAPREDUCE=1 ;;
        --skip-experiments)    SKIP_EXPERIMENTS=1 ;;
        *) echo "[ERROR] Nieznany argument: $arg"; exit 1 ;;
    esac
done

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
step() { echo -e "\n${CYAN}===> $*${NC}"; }

t0=$(date +%s)
step "PIPELINE START ($(date '+%Y-%m-%d %H:%M:%S'))"

# ── 1. Infrastruktura ────────────────────────────────────────────────────────

if [ $SKIP_INFRASTRUCTURE -eq 0 ]; then
    step "1/5 Infrastruktura: Hadoop/Spark + Nominatim"
    info "Uruchamiam klaster Hadoop/Spark..."
    bash "$ROOT/infrastructure/setup_hadoop.sh" "$DATA"
    info "Uruchamiam Nominatim..."
    bash "$ROOT/infrastructure/setup_nominatim.sh"
else
    warn "1/5 Pomijam infrastrukture (--skip-infrastructure)"
fi

# ── 2. Pobieranie danych ─────────────────────────────────────────────────────

if [ $SKIP_ACQUISITION -eq 0 ]; then
    step "2/5 Pobieranie danych nieruchomosci"
    python3 "$ROOT/scripts/acquisition/deweloperuch.py"
else
    warn "2/5 Pomijam pobieranie danych (--skip-acquisition)"
fi

# ── 3. Przetwarzanie lokalne ─────────────────────────────────────────────────

if [ $SKIP_PROCESSING -eq 0 ]; then
    step "3/5 Przetwarzanie danych"
    info "Ekstrakcja i normalizacja CSV..."
    python3 "$ROOT/scripts/processing/extract.py" "$DATA/deweloperka/dane" "$DATA/real_estate_merged"
    info "Geocodowanie adresow (Nominatim)..."
    python3 "$ROOT/scripts/processing/geocode_real_estate.py" "$DATA/real_estate_merged" "$DATA/real_estate_geocoded"
else
    warn "3/5 Pomijam przetwarzanie (--skip-processing)"
fi

# ── 4. MapReduce pipeline ────────────────────────────────────────────────────

if [ $SKIP_MAPREDUCE -eq 0 ]; then
    step "4/5 MapReduce pipeline (E1.1 -> E1.5 -> E1.2 -> E1.3 -> E2 -> E3)"
    bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" all
else
    warn "4/5 Pomijam MapReduce (--skip-mapreduce)"
fi

# ── 5. Eksperymenty ──────────────────────────────────────────────────────────

if [ $SKIP_EXPERIMENTS -eq 0 ]; then
    step "5/5 Eksperymenty (combiner, reducery)"
    bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" exp_combiner
    bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" exp_reducers
else
    warn "5/5 Pomijam eksperymenty (--skip-experiments)"
fi

# ── Podsumowanie ─────────────────────────────────────────────────────────────

t1=$(date +%s)
elapsed=$((t1 - t0))
step "PIPELINE GOTOWY w ${elapsed}s"
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"
