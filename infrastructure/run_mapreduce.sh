#!/bin/bash
# run_mapreduce.sh - uruchamia joby MapReduce na klastrze Hadoop
# Uzycie: ./run_mapreduce.sh [sciezka_do_projektu] [sciezka_do_danych] [job_flag]
# Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all
# Przyklad: ./run_mapreduce.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(realpath "${1:-$SCRIPT_DIR/..}")"
DATA="$(realpath "${2:-$PROJECT_DIR/data}")"
JOB_FLAG="${3:-all}"
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

hdfs_rm() { docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash $1 2>/dev/null || true"; }

# Uruchamia yarn jar i ignoruje blad Job History Server (Unknown Job).
# Weryfikuje sukces przez obecnosc katalogu wyjsciowego na HDFS.
yarn_job() {
  local output_dir="$1"; shift
  docker exec "$CONTAINER" bash -c "yarn jar $*" 2>&1 || true
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $output_dir" \
    || error "Job zakonczyl sie bledem - brak katalogu wyjsciowego: $output_dir"
}

# ── Weryfikacja srodowiska ────────────────────────────────────────────────────

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie dziala. Uruchom klaster przez start.sh"

HDFS_REPORT=$(docker exec "$CONTAINER" hdfs dfsadmin -report 2>&1 || true)
echo "$HDFS_REPORT" | grep -q "Live datanodes" \
  || error "HDFS nie dziala. Sprawdz: docker logs master"

docker exec "$CONTAINER" bash -c "test -f $JAR_HDFS_PATH" 2>/dev/null || {
  info "JAR nie znaleziony na masterze - szukam w target/..."
  JAR_LOCAL=$(find "$PROJECT_DIR/mapreduce/target" "$PROJECT_DIR/target" -maxdepth 1 \
              -name "mapreduce-*-SNAPSHOT.jar" ! -name "original-*" 2>/dev/null | head -1)
  [ -n "$JAR_LOCAL" ] || error "Brak JAR w $PROJECT_DIR/mapreduce/target/. Uruchom najpierw: cd mapreduce && mvn clean package -DskipTests"
  docker exec "$CONTAINER" mkdir -p /data/hadoop
  docker cp "$JAR_LOCAL" "$CONTAINER:$JAR_HDFS_PATH"
  info "JAR skopiowany: $JAR_HDFS_PATH"
}

# ── Joby ─────────────────────────────────────────────────────────────────────

run_e1_1() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -e $HDFS_GTFS/stop_times.txt" \
    || error "Brak danych GTFS na HDFS ($HDFS_GTFS/stop_times.txt). Uruchom start.sh"

  hdfs_rm "$HDFS_T1_1"
  yarn_job "$HDFS_T1_1" $JAR_HDFS_PATH gtfs.GtfsE11 \
    $HDFS_GTFS/stop_times.txt $HDFS_GTFS $HDFS_T1_1
}

run_e1_5() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  hdfs_rm "$HDFS_T1_5"
  yarn_job "$HDFS_T1_5" $JAR_HDFS_PATH gtfs.GtfsE15 $HDFS_T1_1 $HDFS_T1_5
}

run_exp_combiner() {
  info "=== EKSPERYMENT B: Combiner on/off (1 reducer) ==="

  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  hdfs_rm "${HDFS_T1_5}_exp_comb_off"
  run_timed "E1.5_combiner_OFF" yarn_job "${HDFS_T1_5}_exp_comb_off" \
    $JAR_HDFS_PATH gtfs.GtfsE15 $HDFS_T1_1 ${HDFS_T1_5}_exp_comb_off 1 false

  hdfs_rm "${HDFS_T1_5}_exp_comb_on"
  run_timed "E1.5_combiner_ON" yarn_job "${HDFS_T1_5}_exp_comb_on" \
    $JAR_HDFS_PATH gtfs.GtfsE15 $HDFS_T1_1 ${HDFS_T1_5}_exp_comb_on 1 true

  info "=== PODSUMOWANIE eksperymentu B ==="
  grep "TIMING.*combiner" "$LOG_FILE" || true
}

run_exp_reducers() {
  info "=== EKSPERYMENT C: Liczba reducerow (bez combinera) ==="

  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_1" \
    || { info "Brak t1_1 - uruchamiam E1.1 najpierw..."; run_timed "E1.1" run_e1_1; }

  for N in 1 2 3; do
    hdfs_rm "${HDFS_T1_5}_exp_r${N}"
    run_timed "E1.5_reducers_${N}" yarn_job "${HDFS_T1_5}_exp_r${N}" \
      $JAR_HDFS_PATH gtfs.GtfsE15 $HDFS_T1_1 ${HDFS_T1_5}_exp_r${N} ${N} false
  done

  info "=== PODSUMOWANIE eksperymentu C ==="
  grep "TIMING.*reducers" "$LOG_FILE" || true
}

run_e1_2() {
  info "==> Konwersja OSM Parquet -> CSV (PySpark)..."
  docker cp "$PROJECT_DIR/scripts/processing/osm_to_csv.py" spark:/tmp/osm_to_csv.py
  docker exec spark bash -c "
    /usr/local/spark-3.4.0-bin-hadoop3/bin/spark-submit \
      --master yarn \
      --deploy-mode client \
      /tmp/osm_to_csv.py
  "

  hdfs_rm "$HDFS_T1_2"
  yarn_job "$HDFS_T1_2" $JAR_HDFS_PATH gtfs.OsmE12 $HDFS_OSM_CSV $HDFS_T1_2
}

run_e1_3() {
  if docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_RE_GEOCODED" 2>/dev/null; then
    info "==> Dane geocodowane juz sa na HDFS ($HDFS_RE_GEOCODED) - pomijam geocoding i upload."
  else
    info "==> Geocoding nieruchomosci (Nominatim)..."
    python3 "$PROJECT_DIR/scripts/processing/geocode_real_estate.py" \
      "$DATA/real_estate_merged" \
      "$DATA/real_estate_geocoded"

    info "==> Upload danych geocodowanych do HDFS..."
    find "$DATA/real_estate_geocoded" -type f | while read -r f; do
      rel="${f#$DATA/real_estate_geocoded/}"
      cat "$f" | docker exec -i "$CONTAINER" \
        hdfs dfs -D dfs.replication=1 -put - "$HDFS_RE_GEOCODED/$rel"
    done
    docker exec "$CONTAINER" bash -c "hdfs dfs -setrep -R 3 $HDFS_RE_GEOCODED" || true
  fi

  hdfs_rm "$HDFS_T1_3"
  yarn_job "$HDFS_T1_3" $JAR_HDFS_PATH gtfs.RealEstateE13 $HDFS_RE_GEOCODED $HDFS_T1_3
}

run_e2() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_5" \
    || error "Brak t1_5. Uruchom e1_5 najpierw."
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_2" \
    || error "Brak t1_2. Uruchom e1_2 najpierw."
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T1_3" \
    || error "Brak t1_3. Uruchom e1_3 najpierw."

  hdfs_rm "$HDFS_T2"
  yarn_job "$HDFS_T2" $JAR_HDFS_PATH gtfs.E2Driver $HDFS_T1_5 $HDFS_T1_2 $HDFS_T1_3 $HDFS_T2
}

run_e3() {
  docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $HDFS_T2" \
    || error "Brak t2. Uruchom e2 najpierw."

  hdfs_rm "$HDFS_T3"
  yarn_job "$HDFS_T3" $JAR_HDFS_PATH gtfs.E3Driver $HDFS_T2 $HDFS_T3
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
