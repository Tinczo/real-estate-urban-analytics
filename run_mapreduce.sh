#!/bin/bash
# run_mapreduce.sh - uruchamia joby MapReduce na klastrze Hadoop
# Uzycie: ./run_mapreduce.sh <sciezka_do_projektu> <sciezka_do_danych> <job_flag>
# Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all
# Przyklad: ./run_mapreduce.sh . ~/data exp_combiner
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Uzycie: $0 <sciezka_do_projektu> <sciezka_do_danych> <job_flag>"
  echo "Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all"
  exit 1
fi

PROJECT_DIR="$(realpath "$1")"
DATA="$(realpath "$2")"
JOB_FLAG="$3"
CONTAINER="master"

# Sciezka JAR na masterze (tam gdzie start.sh/start.ps1 go kopiuje)
JAR_HDFS_PATH="/data/hadoop/mapreduce-1.0-SNAPSHOT.jar"

# Sciezki HDFS
HDFS_GTFS="/gtfs"
HDFS_OSM_CSV="/osm_csv"
HDFS_RE_GEOCODED="/real_estate_geocoded"
HDFS_T1_1="/user/hadoop/t1_1"
HDFS_T1_5="/user/hadoop/t1_5"
HDFS_T1_2="/user/hadoop/t1_2"
HDFS_T1_3="/user/hadoop/t1_3"
HDFS_T2="/user/hadoop/t2"
HDFS_T3="/user/hadoop/t3"

mkdir -p "$DATA/logs"
LOG_FILE="$DATA/logs/mapreduce_$(date +%Y%m%d_%H%M%S).log"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()   { echo -e "${GREEN}[INFO]${NC}  $*" | tee -a "$LOG_FILE"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $*" | tee -a "$LOG_FILE"; }
error()  { echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; exit 1; }
timing() { echo -e "${YELLOW}[TIMING]${NC} $*" | tee -a "$LOG_FILE"; }

run_timed() {
  local label="$1"; shift
  local t0 t1
  t0=$(date +%s)
  info "==> START: $label ($(date '+%H:%M:%S'))"
  "$@"
  t1=$(date +%s)
  timing "$label: $((t1 - t0))s"
}

hdfs_rm() { docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash $1 2>/dev/null || true"; }

# ── Weryfikacja srodowiska ────────────────────────────────────────────────────

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie dziala. Uruchom klaster przez start.sh"

docker exec "$CONTAINER" hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes" \
  || error "HDFS nie dziala. Sprawdz: docker logs master"

docker exec "$CONTAINER" bash -c "test -f $JAR_HDFS_PATH" 2>/dev/null || {
  info "JAR nie znaleziony na masterze - kopiuje z target/..."
  JAR_LOCAL=$(find "$PROJECT_DIR/target" -maxdepth 1 -name "mapreduce-*-SNAPSHOT.jar" \
              ! -name "original-*" | head -1)
  [ -n "$JAR_LOCAL" ] || error "Brak JAR w $PROJECT_DIR/target/. Uruchom: cd mapreduce && mvn clean package -DskipTests"
  docker exec "$CONTAINER" mkdir -p /data/hadoop
  docker cp "$JAR_LOCAL" "$CONTAINER:$JAR_HDFS_PATH"
  info "JAR skopiowany: $JAR_HDFS_PATH"
}

# ── Joby ─────────────────────────────────────────────────────────────────────

run_e1_1() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -e $HDFS_GTFS/stop_times.txt" \
    || error "Brak danych GTFS na HDFS ($HDFS_GTFS/stop_times.txt). Uruchom start.sh"

  hdfs_rm "$HDFS_T1_1"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.GtfsE11 \
      $HDFS_GTFS/stop_times.txt \
      $HDFS_GTFS \
      $HDFS_T1_1
  "
}

run_e1_5() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  hdfs_rm "$HDFS_T1_5"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.GtfsE15 \
      $HDFS_T1_1 \
      $HDFS_T1_5
  "
}

run_exp_combiner() {
  info "=== EKSPERYMENT B: Combiner on/off (1 reducer) ==="

  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  hdfs_rm "${HDFS_T1_5}_exp_comb_off"
  run_timed "E1.5_combiner_OFF" docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.GtfsE15 \
      $HDFS_T1_1 ${HDFS_T1_5}_exp_comb_off 1 false
  "

  hdfs_rm "${HDFS_T1_5}_exp_comb_on"
  run_timed "E1.5_combiner_ON" docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.GtfsE15 \
      $HDFS_T1_1 ${HDFS_T1_5}_exp_comb_on 1 true
  "

  info "=== PODSUMOWANIE eksperymentu B ==="
  grep "TIMING.*combiner" "$LOG_FILE" || true
}

run_exp_reducers() {
  info "=== EKSPERYMENT C: Liczba reducerow (bez combinera) ==="

  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  for N in 1 2 3; do
    hdfs_rm "${HDFS_T1_5}_exp_r${N}"
    run_timed "E1.5_reducers_${N}" docker exec "$CONTAINER" bash -c "
      set -e
      yarn jar $JAR_HDFS_PATH gtfs.GtfsE15 \
        $HDFS_T1_1 ${HDFS_T1_5}_exp_r${N} ${N} false
    "
  done

  info "=== PODSUMOWANIE eksperymentu C ==="
  grep "TIMING.*reducers" "$LOG_FILE" || true
}

run_e1_2() {
  info "==> Konwersja OSM Parquet -> CSV (PySpark)..."
  docker cp "$PROJECT_DIR/scripts/osm_to_csv.py" spark:/tmp/osm_to_csv.py
  docker exec spark bash -c "
    /spark/bin/spark-submit \
      --master yarn \
      --deploy-mode client \
      /tmp/osm_to_csv.py
  "

  hdfs_rm "$HDFS_T1_2"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.OsmE12 \
      $HDFS_OSM_CSV \
      $HDFS_T1_2
  "
}

run_e1_3() {
  info "==> Geocoding nieruchomosci (Nominatim)..."
  python3 "$PROJECT_DIR/scripts/geocode_real_estate.py" \
    "$DATA/real_estate_merged" \
    "$DATA/real_estate_geocoded"

  info "==> Upload danych geocodowanych do HDFS..."
  docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash $HDFS_RE_GEOCODED 2>/dev/null || true"
  find "$DATA/real_estate_geocoded" -type f | while read -r f; do
    rel="${f#$DATA/real_estate_geocoded/}"
    cat "$f" | docker exec -i "$CONTAINER" \
      hdfs dfs -D dfs.replication=1 -put - "$HDFS_RE_GEOCODED/$rel"
  done
  docker exec "$CONTAINER" bash -c "hdfs dfs -setrep -R 3 $HDFS_RE_GEOCODED" || true

  hdfs_rm "$HDFS_T1_3"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.RealEstateE13 \
      $HDFS_RE_GEOCODED \
      $HDFS_T1_3
  "
}

run_e2() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_5" \
    || error "Brak t1_5. Uruchom e1_5 najpierw."
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_2" \
    || error "Brak t1_2. Uruchom e1_2 najpierw."
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_3" \
    || error "Brak t1_3. Uruchom e1_3 najpierw."

  hdfs_rm "$HDFS_T2"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.E2Driver \
      $HDFS_T1_5 $HDFS_T1_2 $HDFS_T1_3 $HDFS_T2
  "
}

run_e3() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T2" \
    || error "Brak t2. Uruchom e2 najpierw."

  hdfs_rm "$HDFS_T3"
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $JAR_HDFS_PATH gtfs.E3Driver \
      $HDFS_T2 $HDFS_T3
  "
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

info "Uruchamianie: $JOB_FLAG"

case "$JOB_FLAG" in
  e1_1)          run_timed "E1.1" run_e1_1 ;;
  e1_5)          run_timed "E1.5" run_e1_5 ;;
  e1_2)          run_timed "E1.2" run_e1_2 ;;
  e1_3)          run_timed "E1.3" run_e1_3 ;;
  e2)            run_timed "E2"   run_e2   ;;
  e3)            run_timed "E3"   run_e3   ;;
  exp_combiner)  run_exp_combiner          ;;
  exp_reducers)  run_exp_reducers          ;;
  all)
    run_timed "E1.1" run_e1_1
    run_timed "E1.5" run_e1_5
    run_timed "E1.2" run_e1_2
    run_timed "E1.3" run_e1_3
    run_timed "E2"   run_e2
    run_timed "E3"   run_e3
    ;;
  *)
    error "Nieznana flaga: '$JOB_FLAG'. Dostepne: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all"
    ;;
esac

info "Gotowe. Log: $LOG_FILE"
timing "Podsumowanie czasow:"
grep "\[TIMING\]" "$LOG_FILE" || true
