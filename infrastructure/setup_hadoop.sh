#!/bin/bash
set -e

WORKSPACE_DIR="$HOME/workspace"
CMD="./compose-up.sh 3.3.5 3.4.0 3 $WORKSPACE_DIR $WORKSPACE_DIR/hadoop_data $WORKSPACE_DIR/hadoop_logs $WORKSPACE_DIR/spark_logs"
CONTAINER="master"
DATA="$(realpath "$1")"

datasets=("gtfs" "osm" "real_estate_merged")

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }

mkdir -p "$WORKSPACE_DIR"
cd spark/docker-script/hadoop

./compose-down.sh
tmux has-session -t spark 2>/dev/null || tmux new-session -d -s spark "$CMD"

info "Czekam na gotowość HDFS..."
while ! docker exec "$CONTAINER" hdfs dfsadmin -report > /dev/null 2>&1; do
  sleep 5
done

docker exec "$CONTAINER" hdfs dfsadmin -safemode wait

for dataset in "${datasets[@]}"; do
  local_dir="$DATA/$dataset"
  
  if [ ! -d "$local_dir" ]; then
    info "${RED}Dataset $dataset nie istnieje w $DATA, pomijam.${NC}"
    continue
  fi

  info "Streaming datasetu: $dataset na HDFS"

  find "$local_dir" -type f | while read -r local_file; do
    relative_path="${local_file#$DATA/}"
    hdfs_path="/$relative_path"
    hdfs_dir=$(dirname "$hdfs_path")

    docker exec "$CONTAINER" hdfs dfs -mkdir -p "$hdfs_dir"

    cat "$local_file" | docker exec -i "$CONTAINER" hdfs dfs -D dfs.replication=1 -put - "$hdfs_path"
    sleep 0.2
    echo -e "${YELLOW}Przesłano:${NC} $relative_path"
  done
done

info "Współczynnik replikacji na 3 dla wszystkich przesłanych danych..."
docker exec "$CONTAINER" hdfs dfs -setrep -R 3 /gtfs /osm /real_estate_merged
info "${GREEN}Akwizycja zakończona sukcesem!${NC}"