#!/bin/bash
# Skrypt do porównywania wydajności MapReduce vs PySpark (3 przebiegi każdego)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(realpath "$SCRIPT_DIR/..")"
LOGS_DIR="$PROJECT_DIR/data/logs"

DATETIME=$(date +%Y%m%d_%H%M%S)
echo "=== Rozpoczęcie eksperymentu porównawczego (3 przebiegi) ==="

for i in {1..3}; do
  echo ">>> PRZEBIEG $i: MapReduce <<<"
  bash "$PROJECT_DIR/rerun.sh" --engine=mapreduce 2>&1 | tee "$LOGS_DIR/cmp_mapreduce_${DATETIME}_run${i}.log"
  
  echo ">>> PRZEBIEG $i: PySpark <<<"
  bash "$PROJECT_DIR/rerun.sh" --engine=spark 2>&1 | tee "$LOGS_DIR/cmp_pyspark_${DATETIME}_run${i}.log"
  
  echo ">>> PRZEBIEG $i: PySpark PROD <<<"
  bash "$PROJECT_DIR/rerun.sh" --engine=spark-prod 2>&1 | tee "$LOGS_DIR/cmp_pyspark_prod_${DATETIME}_run${i}.log"
done

echo "=== Zakończono eksperymenty ==="
