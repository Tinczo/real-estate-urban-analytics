#!/bin/bash
# pipeline.sh - master skrypt: stawia infrastrukture, przetwarza dane, uruchamia potok MapReduce
# Uzycie: ./pipeline.sh [--skip-infrastructure] [--skip-acquisition] [--skip-processing] [--skip-mapreduce] [--skip-experiments] [--engine=mapreduce|spark]
# Przyklad: ./pipeline.sh
# Przyklad (tylko przetwarzanie): ./pipeline.sh --skip-infrastructure --skip-acquisition --engine=spark
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR"
DATA="$ROOT/data"

SKIP_INFRASTRUCTURE=0
SKIP_ACQUISITION=0
SKIP_PROCESSING=0
SKIP_MAPREDUCE=0
SKIP_EXPERIMENTS=0

ENGINE="mapreduce"

for arg in "$@"; do
    case "$arg" in
        --skip-infrastructure) SKIP_INFRASTRUCTURE=1 ;;
        --skip-acquisition)    SKIP_ACQUISITION=1 ;;
        --skip-processing)     SKIP_PROCESSING=1 ;;
        --skip-mapreduce)      SKIP_MAPREDUCE=1 ;;
        --skip-experiments)    SKIP_EXPERIMENTS=1 ;;
        --engine=*)            ENGINE="${arg#*=}" ;;
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

# ── 4. Pipeline ──────────────────────────────────────────────────────────────

if [ $SKIP_MAPREDUCE -eq 0 ]; then
    step "4/5 Pipeline (Engine: $ENGINE)"

    # Upload brakujacych danych na HDFS
    for hdfsDir in "gtfs" "osm" "real_estate_geocoded"; do
        localDir="$DATA/$hdfsDir"
        if [ ! -d "$localDir" ]; then continue; fi
        
        if ! docker exec master hdfs dfs -test -d "/$hdfsDir" >/dev/null 2>&1; then
            files=()
            while IFS=  read -r -d $'\0'; do
                files+=("$REPLY")
            done < <(find "$localDir" -type f -print0)
            
            totalMB=$(du -sm "$localDir" 2>/dev/null | awk '{print $1}' || echo 0)
            info "Uploaduje: $hdfsDir (${#files[@]} plikow, ~${totalMB} MB)..."
            
            i=0
            for file in "${files[@]}"; do
                i=$((i+1))
                rel="${file#$DATA/}"
                hdfsDir2="/$(dirname "$rel")"
                if [ "$hdfsDir2" = "/." ]; then hdfsDir2="/"; fi
                sizeMB=$(du -sm "$file" 2>/dev/null | awk '{print $1}' || echo 0)
                echo -e "\033[1;30m  [$i/${#files[@]}] $(basename "$file") (~${sizeMB} MB)\033[0m"
                docker exec master hdfs dfs -mkdir -p "$hdfsDir2" >/dev/null 2>&1 || true
                docker cp "$file" "master:/tmp/_upload_tmp" >/dev/null 2>&1 || true
                docker exec master hdfs dfs -D dfs.replication=1 -put -f /tmp/_upload_tmp "/$rel" >/dev/null 2>&1 || true
            done
            docker exec master hdfs dfs -setrep -R 3 "/$hdfsDir" >/dev/null 2>&1 || true
            info "  $hdfsDir - gotowe."
        else
            info "  $hdfsDir - juz na HDFS, pomijam."
        fi
    done

    if [ "$ENGINE" = "spark" ] || [ "$ENGINE" = "pyspark" ]; then
        bash "$ROOT/infrastructure/run_pyspark.sh" "$ROOT" "$DATA" all
    elif [ "$ENGINE" = "spark-prod" ]; then
        bash "$ROOT/infrastructure/run_pyspark.sh" "$ROOT" "$DATA" prod
    else
        # JAR
        JAR=$(find "$ROOT/mapreduce/target" -maxdepth 1 -name "mapreduce-*-SNAPSHOT.jar" ! -name "original-*" 2>/dev/null | head -n 1 || true)
        if [ -z "$JAR" ]; then
            info "Buduje JAR (mvn clean package)..."
            mvn -f "$ROOT/mapreduce/pom.xml" clean package -DskipTests -q
            JAR=$(find "$ROOT/mapreduce/target" -maxdepth 1 -name "mapreduce-*-SNAPSHOT.jar" ! -name "original-*" 2>/dev/null | head -n 1 || true)
        fi
        if [ -n "$JAR" ]; then
            docker cp "$JAR" "master:/data/hadoop/mapreduce-1.0-SNAPSHOT.jar" >/dev/null 2>&1 || true
            info "JAR skopiowany na mastera."
        fi

        bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" all
    fi
else
    warn "4/5 Pomijam Pipeline (--skip-mapreduce)"
fi

# ── 5. Eksperymenty ──────────────────────────────────────────────────────────

if [ $SKIP_EXPERIMENTS -eq 0 ]; then
    step "5/5 Eksperymenty (Engine: $ENGINE)"
    if [ "$ENGINE" = "spark" ] || [ "$ENGINE" = "pyspark" ] || [ "$ENGINE" = "spark-prod" ]; then
        bash "$ROOT/infrastructure/run_experiments_pyspark.sh" "$ROOT" "$DATA"
    else
        bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" exp_combiner
        bash "$ROOT/infrastructure/run_mapreduce.sh" "$ROOT" "$DATA" exp_reducers
    fi
else
    warn "5/5 Pomijam eksperymenty (--skip-experiments)"
fi

# ── Podsumowanie ─────────────────────────────────────────────────────────────

t1=$(date +%s)
elapsed=$((t1 - t0))
step "PIPELINE GOTOWY w ${elapsed}s"
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"
