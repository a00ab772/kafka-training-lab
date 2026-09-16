#!/bin/bash

if [ -z $1 ]
then
    echo """

How to use it:

./produceRecordsToMonitoringTopic.sh <num of messages>

Example:

./produceRecordsToMonitoringTopic.sh 10

 """
  exit 1
fi
 
PIPE=/tmp/pipe.txt
NUM_MESSAGES=$1

write_to_pipe() {
    exec 3>$PIPE
        for i in $(seq $NUM_MESSAGES); do
        echo $i:"Message $i" >&3
    done
    exec 3>&-
}

write_to_kafka() {
    BEGIN=$(date +%s)
        cat $PIPE | $CONFLUENT_HOME/bin/kafka-console-producer \
          --request-required-acks 1 \
          --property "parse.key=true" \
          --property "key.separator=:" \
          --broker-list localhost:9092 \
          --topic monitoring
    END=$(date +%s)
    DURATION=$((END - BEGIN))
    AVERAGE=$((NUM_MESSAGES * 60 / DURATION))
    echo "It took $DURATION seconds to produce $NUM_MESSAGES messages"
    echo "Average productivity was $AVERAGE messages/minute"
    exit 0
}

rm -rf $PIPE
mkfifo $PIPE

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

write_to_pipe & write_to_kafka & read
