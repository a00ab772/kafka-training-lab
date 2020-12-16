#!/bin/bash

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/
$CONFLUENT_HOME/bin/confluent local services stop

sudo docker-compose -f ./KafkaStreamsExercises/testkafkaconnect/code/docker-compose.yml up -d
sudo docker-compose -f ./KafkaStreamsExercises/testkafkaconnect/code/docker-compose.yml  ps
nc -vz localhost 2181
nc -vz localhost 9092
echo ruok | nc localhost 2181
