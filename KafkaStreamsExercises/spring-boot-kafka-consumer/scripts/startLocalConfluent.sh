#!/bin/bash

docker-compose -f ./docker-compose.yml stop
export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

cd ./security/ssl/scripts
./run.sh

# sleep 15
# echo ruok | nc localhost 2181
# echo
# nc -vz localhost 2181
# nc -vz localhost 9092
# nc -vz localhost 9093

./deleteTopics.sh
./createTopics.sh


