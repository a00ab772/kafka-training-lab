#!/bin/bash

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

for i in `$CONFLUENT_HOME/bin/kafka-topics --list --zookeeper localhost:2181| grep -v __consumer_offsets| grep -v _confluent`; do
   $CONFLUENT_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic $i;
done


