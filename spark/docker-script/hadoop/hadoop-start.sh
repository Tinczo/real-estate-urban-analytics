#!/bin/bash

flag=$1

if [ -z $flag ]
then
  echo "Some parameter value is empty. Usage: hadoop-start.sh <flag [ start | restart ]>"
  exit 1
fi

if [ $flag != "start" ] && [ $flag != "restart" ] ; then
  echo "Wrong flag detected. Available flag is [ start | restart ]"
  exit 1
fi

echo "Start Hadoop service."
for node in master slave1 slave2 slave3; do
  docker exec -u 0 $node bash -c "echo 'export HADOOP_OPTS=\"-XX:-UseContainerSupport\"' >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh"
done
if [ $flag = "restart" ]
then
  docker exec master bash -c "/sh/restart-all.sh"
elif [ $flag = "start" ]
then
  docker exec master bash -c "/sh/start-all.sh"
fi