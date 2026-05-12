#!/bin/bash
# setup_hadoop.sh - uruchamia klaster Hadoop/Spark i uploaduje dane do HDFS
# Uzycie: ./setup_hadoop.sh [DataDir]
# Przyklad: ./setup_hadoop.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${1:-$SCRIPT_DIR/../data}"
DATA_DIR="$(realpath "$DATA_DIR")"

WORKSPACE_DIR="$HOME/workspace"
CONTAINER="master"
COMPOSE_DIR="$SCRIPT_DIR/../spark/docker-script/hadoop"
datasets=("gtfs" "osm" "real_estate_merged")

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; GRAY='\033[1;30m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

mkdir -p "$WORKSPACE_DIR"
cd "$COMPOSE_DIR"

if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${CONTAINER}$"; then
    info "Klaster juz dziala - pomijam uruchamianie."
else
    info "Uruchamiam klaster (Hadoop 3.3.5 + Spark 3.4.0, 3 slavey)..."
    bash compose-up.sh 3.3.5 3.4.0 3 "$WORKSPACE_DIR" "$WORKSPACE_DIR/hadoop_data" "$WORKSPACE_DIR/hadoop_logs" "$WORKSPACE_DIR/spark_logs" >/dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        err "compose-up.sh zakonczyl sie bledem. Sprawdz logi Docker."
        exit 1
    fi
fi

info "Czekam na gotowosc HDFS..."
attempts=0
while true; do
    if docker exec "$CONTAINER" hdfs dfsadmin -report >/dev/null 2>&1; then
        break
    fi
    attempts=$((attempts + 1))
    if [ $attempts -gt 60 ]; then
        err "HDFS nie wystartowal po 5 minutach. Sprawdz: docker logs master"
        exit 1
    fi
    echo -e "${GRAY}  ... czekam ($attempts/60)${NC}"
    sleep 5
done

info "Czekam na wyjscie z trybu safe mode..."
docker exec "$CONTAINER" hdfs dfsadmin -safemode wait >/dev/null 2>&1

info "HDFS gotowy. Uploaduje dane..."

for dataset in "${datasets[@]}"; do
    localDir="$DATA_DIR/$dataset"
    
    if [ ! -d "$localDir" ]; then
        warn "Dataset '$dataset' nie istnieje w $DATA_DIR - pomijam."
        continue
    fi

    files=()
    while IFS=  read -r -d $'\0'; do
        files+=("$REPLY")
    done < <(find "$localDir" -type f -print0)
    
    info "Uploaduje: $dataset (${#files[@]} plikow)..."
    
    for file in "${files[@]}"; do
        relativePath="${file#$DATA_DIR/}"
        hdfsPath="/$relativePath"
        hdfsDir="/$(dirname "$relativePath")"
        if [ "$hdfsDir" = "/." ]; then hdfsDir="/"; fi
        
        docker exec "$CONTAINER" hdfs dfs -mkdir -p "$hdfsDir" >/dev/null 2>&1 || true
        cat "$file" | docker exec -i "$CONTAINER" hdfs dfs -D dfs.replication=1 -put - "$hdfsPath" >/dev/null 2>&1 || true
    done
    info "  $dataset - gotowe."
done

info "Ustawiam wspolczynnik replikacji na 3..."
existingDirs=""
for dataset in "${datasets[@]}"; do
    if docker exec "$CONTAINER" hdfs dfs -test -d "/$dataset" >/dev/null 2>&1; then
        existingDirs="$existingDirs /$dataset"
    fi
done

if [ -n "$existingDirs" ]; then
    docker exec "$CONTAINER" hdfs dfs -setrep -R 3 $existingDirs >/dev/null 2>&1 || true
fi

info "Gotowe! Klaster dziala."
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"