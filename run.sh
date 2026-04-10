#!/bin/bash
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Użycie: $0 <ścieżka_do_projektu> <ścieżka_do_danych>"
  exit 1
fi

PROJECT_DIR="$(realpath "$1")"
CONTAINER="master"
MAIN_CLASS="GtfsPipeline"
DATA="$(realpath "$2")"
GTFS_DIR="$DATA/gtfs"
MASTER_DATA="/tmp/data"
MASTER_GTFS="$MASTER_DATA/gtfs"
MASTER_JAR="$MASTER_DATA/mapreduce-1.0-SNAPSHOT.jar"

HDFS_INPUT="/user/hadoop/gtfs"
HDFS_OUT_E11="/user/hadoop/t1_1"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[ -d "$PROJECT_DIR" ]         || error "Brak katalogu projektu: $PROJECT_DIR"
[ -f "$PROJECT_DIR/pom.xml" ] || error "Brak pom.xml w $PROJECT_DIR"
[ -d "$GTFS_DIR" ]            || error "Brak katalogu gtfs/ w $DATA"

for f in stop_times.txt trips.txt routes.txt calendar.txt stops.txt; do
  [ -f "$GTFS_DIR/$f" ] || error "Brak pliku: $GTFS_DIR/$f"
done

cd hadoop-eco/docker-script
./compose-down.sh
./compose-up.sh 3.3.0 3 /tmp/hadoop /tmp/hadoop_logs /tmp/hbase_logs /tmp/hive_logs /tmp/sqoop_logs mariadb /Users/Shared/workspace/docker-ws/maria-data
tmux has-session -t hadoop 2>/dev/null || tmux new-session -d -s hadoop './hadoop-start.sh start'
cd ../..

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" \
  || error "Kontener '$CONTAINER' nie działa"

info "==> [1/3] Budowanie fat jar..."
cd "$PROJECT_DIR"
mvn clean package -DskipTests -q || error "Maven build nie powiódł się"

JAR_PATH=$(find "$PROJECT_DIR/target" -maxdepth 1 -name "*.jar" \
           ! -name "*-sources.jar" ! -name "*-original.jar" | head -1)
[ -n "$JAR_PATH" ] || error "Nie znaleziono jara w target/"
info "Jar: $JAR_PATH"
cp "$JAR_PATH" "$DATA/"

info "==> [2/3] Wgrywanie danych do HDFS..."
docker cp $DATA $CONTAINER:$MASTER_DATA
docker exec "$CONTAINER" bash -c "
  set -e
  hdfs dfsadmin -safemode wait

  hdfs dfs -rm -r -f $HDFS_OUT_E11 2>/dev/null || true

  if ! hdfs dfs -test -d $HDFS_INPUT; then
    hdfs dfs -mkdir -p $HDFS_INPUT
    hdfs dfs -put $MASTER_GTFS/* $HDFS_INPUT/
    echo 'Dane GTFS wgrane do HDFS'
  else
    echo 'Dane GTFS już w HDFS, pomijam'
  fi

  echo 'Pliki w HDFS:'
  hdfs dfs -ls $HDFS_INPUT
"

info "==> [3/3] Uruchamianie MapReduce job..."
docker exec "$CONTAINER" bash -c "
  set -e
  yarn jar $MASTER_JAR $MAIN_CLASS \
    $HDFS_INPUT/stop_times.txt \
    $HDFS_INPUT \
    $HDFS_OUT_E11
"

info "==> Kopiowanie wyników T1.1:"
docker exec master bash -c "hdfs dfs -get /user/hadoop/t1_1 /data/hadoop/wyniki_t1_1"