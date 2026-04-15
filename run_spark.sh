#!/bin/bash
set -e

WORKSPACE_DIR="$HOME/workspace/spark-notebook"
TADEUSZ=https://wolnelektury.pl/media/book/txt/pan-tadeusz.txt
CMD='./compose-up.sh 3.3.5 3.4.0 3 $HOME/workspace/spark-notebook /tmp/hadoop /tmp/hadoop_logs /tmp/spark_logs'
MASTER_DATA="/tmp/data"

mkdir -p "$WORKSPACE_DIR"
if [ ! -f "$WORKSPACE_DIR/pan_tadeusz.txt" ]; then
    wget -q -O "$WORKSPACE_DIR/pan_tadeusz.txt" $TADEUSZ
fi

if [ -f "words.ipynb" ]; then
    cp "words.ipynb" "$WORKSPACE_DIR/"
else
    echo "Nie znaleziono notatnika."
    exit 1
fi


cd spark/docker-script/hadoop

./compose-down.sh
tmux has-session -t spark 2>/dev/null || tmux new-session -d -s spark $CMD
while ! docker exec master hdfs dfsadmin -report > /dev/null 2>&1; do
  sleep 5
done
docker exec -u 0 jupyter-lab bash -c "
  curl https://bootstrap.pypa.io/get-pip.py | /usr/bin/python3.9
  /usr/bin/python3.9 -m pip install ipykernel
  /usr/bin/python3.9 -m ipykernel install --name python39 --display-name 'Python 3.9'
  /usr/bin/python3.9 -m pip install pyspark==3.4.0
"
echo "wait"
docker exec master bash -c "
  set -e
  hdfs dfsadmin -safemode wait
"
docker cp $WORKSPACE_DIR master:$MASTER_DATA/
docker exec master hdfs dfs -D dfs.replication=3 -put $MASTER_DATA/pan_tadeusz.txt /



./jupyter-start.sh
