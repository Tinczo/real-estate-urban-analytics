#!/bin/bash
# run_pyspark.sh - uruchamia pipeline w PySpark na klastrze Hadoop/Spark
# Uzycie: ./run_pyspark.sh [sciezka_do_projektu] [sciezka_do_danych] [job_flag]
# Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, all
# Przyklad: ./run_pyspark.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(realpath "${1:-$SCRIPT_DIR/..}")"
DATA="$(realpath "${2:-$PROJECT_DIR/data}")"
JOB_FLAG="${3:-all}"
CONTAINER="spark"
MASTER_CONTAINER="master"

# Skrypt python z pipeline'em
PYSPARK_SCRIPT="/tmp/spark_pipeline.py"

# Sciezki HDFS (z uzyciem py_ suffix dla odroznienia)
HDFS_GTFS="/gtfs"
HDFS_OSM_CSV="/osm_csv"
HDFS_RE_GEOCODED="/real_estate_geocoded"
HDFS_T1_1="/user/hadoop/t1_1_py"
HDFS_T1_5="/user/hadoop/t1_5_py"
HDFS_T1_2="/user/hadoop/t1_2_py"
HDFS_T1_3="/user/hadoop/t1_3_py"
HDFS_T2="/user/hadoop/t2_py"
HDFS_T3="/user/hadoop/t3_py"

mkdir -p "$DATA/logs"
LOG_FILE="$DATA/logs/pyspark_$(date +%Y%m%d_%H%M%S).log"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_plain() { sed 's/\x1b\[[0-9;]*m//g' >> "$LOG_FILE"; }
info()   { echo -e "${GREEN}[INFO]${NC}  $*" | tee >(log_plain); }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $*" | tee >(log_plain); }
error()  { echo -e "${RED}[ERROR]${NC} $*" | tee >(log_plain); exit 1; }
timing() { echo -e "${YELLOW}[TIMING]${NC} $*" | tee >(log_plain); }

run_timed() {
  local label="$1"; shift
  local t0 t1
  t0=$(date +%s)
  info "==> START: $label ($(date '+%H:%M:%S'))"
  "$@"
  t1=$(date +%s)
  timing "$label: $((t1 - t0))s"
}

hdfs_rm() { docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash $1 2>/dev/null || true"; }

spark_submit() {
  local stage="$1"
  local extra_args="${2:-}"
  docker exec "$CONTAINER" bash -c "
    /usr/local/spark/bin/spark-submit \
      --master yarn \
      --deploy-mode client \
      --driver-memory 2g \
      --executor-memory 2g \
      $PYSPARK_SCRIPT --stage $stage $extra_args
  " 2>&1 | tee >(log_plain) || error "PySpark Job zakonczyl sie bledem dla: $stage"
}

# ── Weryfikacja srodowiska ────────────────────────────────────────────────────

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie dziala. Uruchom klaster przez start.sh"

docker ps --format '{{.Names}}' | grep -q "^${MASTER_CONTAINER}$" \
  || error "Kontener '$MASTER_CONTAINER' nie dziala. Uruchom klaster przez start.sh"

info "Kopiowanie spark_pipeline.py do kontenera spark..."
docker cp "$PROJECT_DIR/spark/src/spark_pipeline.py" "$CONTAINER:$PYSPARK_SCRIPT"

# ── Joby ─────────────────────────────────────────────────────────────────────

run_e1_1() {
  docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -test -e $HDFS_GTFS/stop_times.txt" \
    || error "Brak danych GTFS na HDFS ($HDFS_GTFS/stop_times.txt). Uruchom start.sh"
  hdfs_rm "$HDFS_T1_1"
  spark_submit "e1_1" "--stop_times $HDFS_GTFS/stop_times.txt --gtfs_cache $HDFS_GTFS --t1_1 $HDFS_T1_1"
}

run_e1_5() {
  hdfs_rm "$HDFS_T1_5"
  spark_submit "e1_5" "--t1_1 $HDFS_T1_1 --t1_5 $HDFS_T1_5"
}

run_e1_2() {
  hdfs_rm "$HDFS_T1_2"
  spark_submit "e1_2" "--osm_csv $HDFS_OSM_CSV --t1_2 $HDFS_T1_2"
}

run_e1_3() {
  if docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -test -d $HDFS_RE_GEOCODED" 2>/dev/null; then
    info "==> Dane geocodowane juz sa na HDFS ($HDFS_RE_GEOCODED) - pomijam geocoding i upload."
  else
    info "==> Geocoding nieruchomosci (Nominatim)..."
    python3 "$PROJECT_DIR/scripts/processing/geocode_real_estate.py" \
      "$DATA/real_estate_merged" \
      "$DATA/real_estate_geocoded"

    info "==> Upload danych geocodowanych do HDFS..."
    find "$DATA/real_estate_geocoded" -type f | while read -r f; do
      rel="${f#$DATA/real_estate_geocoded/}"
      cat "$f" | docker exec -i "$MASTER_CONTAINER" \
        hdfs dfs -D dfs.replication=1 -put - "$HDFS_RE_GEOCODED/$rel"
    done
    docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -setrep -R 3 $HDFS_RE_GEOCODED" || true
  fi
  
  hdfs_rm "$HDFS_T1_3"
  spark_submit "e1_3" "--re_geocoded $HDFS_RE_GEOCODED --t1_3 $HDFS_T1_3"
}

run_e2() {
  hdfs_rm "$HDFS_T2"
  spark_submit "e2" "--t1_5 $HDFS_T1_5 --t1_2 $HDFS_T1_2 --t1_3 $HDFS_T1_3 --t2 $HDFS_T2"
}

run_e3() {
  hdfs_rm "$HDFS_T3"
  spark_submit "e3" "--t2 $HDFS_T2 --t3 $HDFS_T3"
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

info "Uruchamianie: $JOB_FLAG"

case "$JOB_FLAG" in
  e1_1) run_timed "E1.1" run_e1_1 ;;
  e1_5) run_timed "E1.5" run_e1_5 ;;
  e1_2) run_timed "E1.2" run_e1_2 ;;
  e1_3) run_timed "E1.3" run_e1_3 ;;
  e2)   run_timed "E2"   run_e2   ;;
  e3)   run_timed "E3"   run_e3   ;;
  all)
    run_timed "E1.1" run_e1_1
    run_timed "E1.5" run_e1_5
    run_timed "E1.2" run_e1_2
    run_timed "E1.3" run_e1_3
    run_timed "E2"   run_e2
    run_timed "E3"   run_e3
    ;;
  *)
    error "Nieznana flaga: '$JOB_FLAG'. Dostepne: e1_1, e1_5, e1_2, e1_3, e2, e3, all"
    ;;
esac

info "Gotowe. Log: $LOG_FILE"
timing "Podsumowanie czasow:"
grep "\[TIMING\]" "$LOG_FILE" || true
