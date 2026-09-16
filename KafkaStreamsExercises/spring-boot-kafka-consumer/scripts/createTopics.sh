#!/bin/bash

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

TopicList="producer-consumer,rental-car-input,rental-car-output,bank-balance-agg,bank-balance-exactly-once,bank-transactions,customers-and-cars,kafka-security-topic,kafka-spring-cloud,kafka-spring-cloud-json,monitoring,streams-plaintext-input,streams-wordcount-output"

IFS=,
for topic in $TopicList;
do
$CONFLUENT_HOME/bin/kafka-topics --zookeeper localhost:2181 \
                --create --topic $topic --partitions 3 --replication-factor 1

$CONFLUENT_HOME/bin/kafka-topics --zookeeper localhost:2181 --describe --topic $topic
done
