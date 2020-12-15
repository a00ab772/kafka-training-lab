#!/bin/bash

docker-compose -f /usr/local/intellij/ProyectosIntelliJ/testkafkaconnect/code/docker-compose.yml stop
export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/
confluent local services kafka start
sleep 5
echo ruok | nc localhost 2181
echo
nc -vz localhost 2181
nc -vz localhost 9092
# nc -vz localhost 9093
# nc -vz localhost 9094

