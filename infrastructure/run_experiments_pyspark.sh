#!/bin/bash
# run_experiments_pyspark.sh - uruchamia eksperymenty w PySpark
# Uzycie: ./run_experiments_pyspark.sh [sciezka_do_projektu] [sciezka_do_danych]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(realpath "${1:-$SCRIPT_DIR/..}")"
DATA="$(realpath "${2:-$PROJECT_DIR/data}")"
CONTAINER="spark"
MASTER_CONTAINER="master"

PYSPARK_SCRIPT="/tmp/spark_pipeline.py"
HDFS_GTFS="/gtfs"
HDFS_T1_1="/user/hadoop/t1_1_py"
HDFS_T1_5="/user/hadoop/t1_5_py"

mkdir -p "$DATA/logs"
LOG_FILE="$DATA/logs/pyspark_experiments_$(date +%Y%m%d_%H%M%S).log"

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

info "Weryfikacja srodowiska..."
docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie dziala. Uruchom klaster przez start.sh"

docker ps --format '{{.Names}}' | grep -q "^${MASTER_CONTAINER}$" \
  || error "Kontener '$MASTER_CONTAINER' nie dziala. Uruchom klaster przez start.sh"

info "Kopiowanie spark_pipeline.py do kontenera spark..."
docker cp "$PROJECT_DIR/spark/src/spark_pipeline.py" "$CONTAINER:$PYSPARK_SCRIPT"

# Przygotowanie danych bazowych (jesli brak)
docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -test -e $HDFS_GTFS/stop_times.txt" \
  || error "Brak danych GTFS na HDFS. Uruchom start.sh"

# EKSPERYMENT 1: Broadcast Join vs SortMerge Join
run_exp_joins() {
  info "=== EKSPERYMENT 1: Broadcast Join vs SortMerge Join (E1.1) ==="
  
  # Z Broadcast
  hdfs_rm "${HDFS_T1_1}_exp_broadcast"
  run_timed "E1.1_BROADCAST_ON" spark_submit "e1_1" \
    "--stop_times $HDFS_GTFS/stop_times.txt --gtfs_cache $HDFS_GTFS --t1_1 ${HDFS_T1_1}_exp_broadcast"

  # Bez Broadcast
  hdfs_rm "${HDFS_T1_1}_exp_nobroadcast"
  run_timed "E1.1_BROADCAST_OFF" spark_submit "e1_1" \
    "--stop_times $HDFS_GTFS/stop_times.txt --gtfs_cache $HDFS_GTFS --t1_1 ${HDFS_T1_1}_exp_nobroadcast --disable_broadcast"

  info "=== PODSUMOWANIE EKSPERYMENTU 1 ==="
  grep "TIMING.*BROADCAST" "$LOG_FILE" || true
}

# EKSPERYMENT 2: Liczba partycji shuffle
run_exp_partitions() {
  info "=== EKSPERYMENT 2: Liczba partycji shuffle (E1.5) ==="
  
  # Upewnij sie, ze E1.1 bazowe istnieje
  docker exec "$MASTER_CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1_base" spark_submit "e1_1" "--stop_times $HDFS_GTFS/stop_times.txt --gtfs_cache $HDFS_GTFS --t1_1 $HDFS_T1_1"; }

  for N in 1 10 50; do
    hdfs_rm "${HDFS_T1_5}_exp_p${N}"
    run_timed "E1.5_PARTITIONS_${N}" spark_submit "e1_5" \
      "--t1_1 $HDFS_T1_1 --t1_5 ${HDFS_T1_5}_exp_p${N} --reducers ${N}"
  done

  info "=== PODSUMOWANIE EKSPERYMENTU 2 ==="
  grep "TIMING.*PARTITIONS" "$LOG_FILE" || true
}

run_exp_joins
run_exp_partitions

info "Eksperymenty zakonczone. Zobacz pelny log: $LOG_FILE"
