#!/bin/bash
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Użycie: $0 <ścieżka_do_projektu> <ścieżka_do_danych> <job_flag>"
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

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

run_e1_1() {
  MASTER_JAR="$MASTER_DATA/mapreduce-1.0-SNAPSHOT.jar"
  MAIN_CLASS="gtfs.GtfsE11"

  info "==> Uruchamianie e1_1 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $MASTER_JAR $MAIN_CLASS \
      $HDFS_INPUT/data/gtfs/stop_times.txt \
      $HDFS_INPUT/data/gtfs \
      $HDFS_OUT_E11
  "

  info "==> Kopiowanie wyników T1.1:"
  docker exec master bash -c "hdfs dfs -get /user/hadoop/t1_1 /data/hadoop/t1_1"
  docker cp /tmp/hadoop/t1_1 $CONTAINER:$MASTER_DATA/
  docker exec master bash -c "
    hdfs dfs -mkdir -p $HDFS_INPUT/data/t1_1
    hdfs dfs -D dfs.replication=3 -put $MASTER_DATA/t1_1 $HDFS_INPUT/data/
  " 
}

run_e1_5() {
  MASTER_JAR="$MASTER_DATA/mapreduce-1.0-SNAPSHOT.jar"
  MAIN_CLASS="gtfs.GtfsE15"

  info "==> Uruchamianie e1_5 MapReduce job..."
  docker exec "$CONTAINER" bash -c "
    set -e
    yarn jar $MASTER_JAR $MAIN_CLASS \
      $HDFS_INPUT/data/t1_1 \
      $HDFS_OUT_E15
  "

  info "==> Kopiowanie wyników T1.5:"
  docker exec master bash -c "hdfs dfs -get /user/hadoop/t1_5 /data/hadoop/t1_5"
}

[ -d "$PROJECT_DIR" ]         || error "Brak katalogu projektu: $PROJECT_DIR"
[ -f "$PROJECT_DIR/pom.xml" ] || error "Brak pom.xml w $PROJECT_DIR"
[ -d "$GTFS_DIR" ]            || error "Brak katalogu gtfs/ w $DATA"

for f in stop_times.txt trips.txt routes.txt calendar.txt stops.txt; do
  [ -f "$GTFS_DIR/$f" ] || error "Brak pliku: $GTFS_DIR/$f"
done

cd hadoop-eco/docker-script
./compose-down.sh
./compose-up.sh 3.3.0 3 /tmp/hadoop /tmp/hadoop_logs /tmp/hbase_logs /tmp/hive_logs /tmp/sqoop_logs mariadb $HOME/workspace/mardia-data
tmux has-session -t hadoop 2>/dev/null || tmux new-session -d -s hadoop './hadoop-start.sh start'
cd ../..

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie działa"

info "==> Budowanie fat jar..."
cd "$PROJECT_DIR"
mvn clean package -DskipTests -q || error "Maven build nie powiódł się"

JAR_PATH=$(find "$PROJECT_DIR/target" -maxdepth 1 -name "*.jar" \
           ! -name "*-sources.jar" ! -name "original-*.jar" | head -1)
[ -n "$JAR_PATH" ] || error "Nie znaleziono jara w target/"
info "Jar: $JAR_PATH"
cp "$JAR_PATH" "$DATA/"

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

case "$JOB_FLAG" in
  e1_1|1.1)
    run_e1_1
    ;;
  e1_5|1.5)
    run_e1_5
    ;;
  all)
    run_e1_1
    run_e1_5
    ;;
  *)
    error "Nieznana flaga zadania: '$JOB_FLAG'. Dostępne opcje: e1_1, e1_5"
    ;;
esac