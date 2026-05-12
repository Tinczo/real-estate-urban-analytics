#!/bin/bash
docker exec master bash -c "hdfs dfs -get /output/pan_tadeusz_count /data/hadoop/pan_tadeusz_count"
