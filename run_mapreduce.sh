#!/bin/bash
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Użycie: $0 <ścieżka_do_projektu> <ścieżka_do_danych> <job_flag>"
  echo "Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all"
  exit 1
fi

JOB_FLAG="$3"
PROJECT_DIR="$(realpath "$1")"
CONTAINER="master"
DATA="$(realpath "$2")"
GTFS_DIR="$DATA/gtfs"
MASTER_DATA="/tmp/data"

HDFS_INPUT="/user/hadoop"
HDFS_OUT_E11="/user/hadoop/t1_1"
HDFS_OUT_E15="/user/hadoop/t1_5"
HDFS_OUT_E12="/user/hadoop/t1_2"
HDFS_OUT_E13="/user/hadoop/t1_3"
HDFS_OUT_E2="/user/hadoop/t2"
HDFS_OUT_E3="/user/hadoop/t3"
HDFS_OSM_CSV="/user/hadoop/data/osm_csv"
HDFS_RE_GEOCODED="/user/hadoop/data/real_estate_geocoded"

mkdir -p "$DATA/logs"
LOG_FILE="$DATA/logs/mapreduce_$(date +%Y%m%d_%H%M%S).log"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*" | tee -a "$LOG_FILE"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*" | tee -a "$LOG_FILE"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; exit 1; }
timing()  { echo -e "${YELLOW}[TIMING]${NC} $*" | tee -a "$LOG_FILE"; }

# Mierzy czas wykonania funkcji i zapisuje do logu
run_timed() {
  local label="$1"; shift
  local t0 t1 elapsed
  t0=$(date +%s)
  info "==> START: $label ($(date '+%H:%M:%S'))"
  "$@"
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  timing "$label: ${elapsed}s"
}

# Pobiera Application ID ostatnio zakończonego joba YARN i zapisuje logi
capture_yarn_logs() {
  local label="$1"
  info "==> Pobieranie logów YARN dla: $label"
  docker exec "$CONTAINER" bash -c "
    APP_ID=\$(yarn application -list -appStates FINISHED 2>/dev/null | grep -v 'Total number' | tail -1 | awk '{print \$1}')
    if [ -n \"\$APP_ID\" ]; then
      echo '=== YARN LOGS: $label (\$APP_ID) ==='
      yarn logs -applicationId \$APP_ID 2>/dev/null || true
    fi
  " >> "$LOG_FILE" 2>&1 || warn "Nie udało się pobrać logów YARN"
}

MASTER_JAR="$MASTER_DATA/mapreduce-1.0-SNAPSHOT.jar"

run_e1_1() {
  info "==> Uruchamianie e1_1 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E11 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.GtfsE11 \
      $HDFS_INPUT/data/gtfs/stop_times.txt \
      $HDFS_INPUT/data/gtfs \
      $HDFS_OUT_E11
  "
  capture_yarn_logs "E1.1"

  info "==> Kopiowanie wyników T1.1:"
  docker exec master bash -c "hdfs dfs -get /user/hadoop/t1_1 /data/hadoop/t1_1"
  docker cp /tmp/hadoop/t1_1 $CONTAINER:$MASTER_DATA/
  docker exec master bash -c "
    hdfs dfs -mkdir -p $HDFS_INPUT/data/t1_1
    hdfs dfs -D dfs.replication=3 -put $MASTER_DATA/t1_1 $HDFS_INPUT/data/
  "
}

run_e1_5() {
  info "==> Uruchamianie e1_5 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E15 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.GtfsE15 \
      $HDFS_INPUT/data/t1_1 \
      $HDFS_OUT_E15
  "
  capture_yarn_logs "E1.5"

  info "==> Kopiowanie wyników T1.5:"
  docker exec master bash -c "hdfs dfs -get /user/hadoop/t1_5 /data/hadoop/t1_5"
}

# Eksperyment B: Combiner włączony / wyłączony (E1.5, 1 reducer)
run_exp_combiner() {
  info "=== EKSPERYMENT B: Combiner on/off ==="

  info "-- Bez kombinera (1 reducer) --"
  docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash ${HDFS_OUT_E15}_exp_comb_off 2>/dev/null || true"
  run_timed "E1.5_combiner_OFF" docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $MASTER_JAR gtfs.GtfsE15 \
      $HDFS_INPUT/data/t1_1 \
      ${HDFS_OUT_E15}_exp_comb_off \
      1 false
  "
  capture_yarn_logs "E1.5_combiner_OFF"

  info "-- Z kombinerem (1 reducer) --"
  docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash ${HDFS_OUT_E15}_exp_comb_on 2>/dev/null || true"
  run_timed "E1.5_combiner_ON" docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $MASTER_JAR gtfs.GtfsE15 \
      $HDFS_INPUT/data/t1_1 \
      ${HDFS_OUT_E15}_exp_comb_on \
      1 true
  "
  capture_yarn_logs "E1.5_combiner_ON"

  info "=== PODSUMOWANIE eksperymentu B ==="
  grep "TIMING.*E1.5_combiner" "$LOG_FILE" || true
}

# Eksperyment C: Liczba reducerów (E1.5, bez kombinera)
run_exp_reducers() {
  info "=== EKSPERYMENT C: Liczba reducerów ==="

  for N in 1 2 3; do
    info "-- $N reducer(y), bez kombinera --"
    docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash ${HDFS_OUT_E15}_exp_r${N} 2>/dev/null || true"
    run_timed "E1.5_reducers_${N}" docker exec "$CONTAINER" bash -c "
      set -e
      yarn jar $MASTER_JAR gtfs.GtfsE15 \
        $HDFS_INPUT/data/t1_1 \
        ${HDFS_OUT_E15}_exp_r${N} \
        ${N} false
    "
    capture_yarn_logs "E1.5_reducers_${N}"
  done

  info "=== PODSUMOWANIE eksperymentu C ==="
  grep "TIMING.*E1.5_reducers" "$LOG_FILE" || true
}

run_e1_2() {
  info "==> Konwersja OSM Parquet → CSV (PySpark)..."
  docker cp "$PROJECT_DIR/scripts/osm_to_csv.py" spark:/tmp/osm_to_csv.py
  docker exec spark bash -c "
    /spark/bin/spark-submit \
      --master yarn \
      --deploy-mode client \
      /tmp/osm_to_csv.py
  "

  info "==> Uruchamianie e1_2 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E12 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.OsmE12 \
      $HDFS_OSM_CSV \
      $HDFS_OUT_E12
  "
  capture_yarn_logs "E1.2"
}

run_e1_3() {
  info "==> Geocoding nieruchomości (Nominatim)..."
  python3 "$PROJECT_DIR/scripts/geocode_real_estate.py" \
    "$DATA/real_estate_merged" \
    "$DATA/real_estate_geocoded"

  info "==> Upload danych geocodowanych do HDFS..."
  docker cp "$DATA/real_estate_geocoded" "$CONTAINER:$MASTER_DATA/real_estate_geocoded"
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -mkdir -p $HDFS_RE_GEOCODED
    hdfs dfs -D dfs.replication=3 -put $MASTER_DATA/real_estate_geocoded/* $HDFS_RE_GEOCODED/
  "

  info "==> Uruchamianie e1_3 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E13 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.RealEstateE13 \
      $HDFS_RE_GEOCODED \
      $HDFS_OUT_E13
  "
  capture_yarn_logs "E1.3"
}

run_e2() {
  info "==> Uruchamianie E2 MapReduce job (Reduce-side Join)..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E2 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.E2Driver \
      $HDFS_OUT_E15 \
      $HDFS_OUT_E12 \
      $HDFS_OUT_E13 \
      $HDFS_OUT_E2
  "
  capture_yarn_logs "E2"

  info "==> Kopiowanie wyników T2:"
  docker exec master bash -c "hdfs dfs -get $HDFS_OUT_E2 /data/hadoop/t2" || true
}

run_e3() {
  info "==> Uruchamianie E3 MapReduce job (Korelacje Pearsona)..."
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -skipTrash $HDFS_OUT_E3 2>/dev/null || true
    yarn jar $MASTER_JAR gtfs.E3Driver \
      $HDFS_OUT_E2 \
      $HDFS_OUT_E3
  "
  capture_yarn_logs "E3"

  info "==> Kopiowanie wyników T3:"
  docker exec master bash -c "hdfs dfs -get $HDFS_OUT_E3 /data/hadoop/t3" || true
}

# ── Walidacja ────────────────────────────────────────────────────────────────

[ -d "$PROJECT_DIR" ]         || error "Brak katalogu projektu: $PROJECT_DIR"
[ -f "$PROJECT_DIR/pom.xml" ] || error "Brak pom.xml w $PROJECT_DIR"
[ -d "$GTFS_DIR" ]            || error "Brak katalogu gtfs/ w $DATA"

for f in stop_times.txt trips.txt routes.txt calendar.txt stops.txt; do
  [ -f "$GTFS_DIR/$f" ] || error "Brak pliku: $GTFS_DIR/$f"
done

# ── Start klastra ─────────────────────────────────────────────────────────────

cd hadoop-eco/docker-script
./compose-down.sh
./compose-up.sh 3.3.0 3 /tmp/hadoop /tmp/hadoop_logs /tmp/hbase_logs /tmp/hive_logs /tmp/sqoop_logs mariadb $HOME/workspace/mardia-data
tmux has-session -t hadoop 2>/dev/null || tmux new-session -d -s hadoop './hadoop-start.sh start'
cd ../..

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie działa"

# ── Build ─────────────────────────────────────────────────────────────────────

info "==> Budowanie fat jar..."
cd "$PROJECT_DIR"
mvn clean package -DskipTests -q || error "Maven build nie powiódł się"

JAR_PATH=$(find "$PROJECT_DIR/target" -maxdepth 1 -name "*.jar" \
           ! -name "*-sources.jar" ! -name "original-*.jar" | head -1)
[ -n "$JAR_PATH" ] || error "Nie znaleziono jara w target/"
info "Jar: $JAR_PATH"
cp "$JAR_PATH" "$DATA/"

# ── Upload danych do HDFS ─────────────────────────────────────────────────────

info "==> Wgrywanie danych do HDFS..."
docker cp $DATA $CONTAINER:$MASTER_DATA
docker exec "$CONTAINER" bash -c "
  set -e
  hdfs dfsadmin -safemode wait
  hdfs dfs -mkdir -p $HDFS_INPUT
  hdfs dfs -D dfs.replication=3 -put $MASTER_DATA $HDFS_INPUT/

  echo 'Pliki w HDFS:'
  hdfs dfs -ls -R $HDFS_INPUT
"

# ── Dispatch ──────────────────────────────────────────────────────────────────

case "$JOB_FLAG" in
  e1_1|1.1)
    run_timed "E1.1" run_e1_1
    ;;
  e1_5|1.5)
    run_timed "E1.5" run_e1_5
    ;;
  e1_2|1.2)
    run_timed "E1.2" run_e1_2
    ;;
  e1_3|1.3)
    run_timed "E1.3" run_e1_3
    ;;
  e2)
    run_timed "E2" run_e2
    ;;
  e3)
    run_timed "E3" run_e3
    ;;
  exp_combiner)
    run_exp_combiner
    ;;
  exp_reducers)
    run_exp_reducers
    ;;
  all)
    run_timed "E1.1" run_e1_1
    run_timed "E1.5" run_e1_5
    run_timed "E1.2" run_e1_2
    run_timed "E1.3" run_e1_3
    run_timed "E2"   run_e2
    run_timed "E3"   run_e3
    ;;
  *)
    error "Nieznana flaga: '$JOB_FLAG'. Dostępne: e1_1, e1_5, e1_2, e1_3, e2, e3, exp_combiner, exp_reducers, all"
    ;;
esac

info "==> Gotowe. Log: $LOG_FILE"
timing "Pełne podsumowanie czasów:"
grep "\[TIMING\]" "$LOG_FILE" || true
