#!/bin/bash

if [ -z $1 ]
then
    echo """

Forma de uso:

./produceRecordsToMonitoringTopicFIFO.sh <num Messages we are going to produce> <partition we are going to write to>

Example: if we want to produce 10 messages to the partition 2 we would write

./produceRecordsToMonitoringTopicFIFO.sh 10 2

 """
  exit 1
fi

PIPE=/tmp/pipe.txt
NUM_MESSAGES=$1
PARTITION=$2
escribe_a_pipe() {
    exec 3>$PIPE
        for i in $(seq $NUM_MESSAGES); do
        echo $PARTITION:"Message $i" >&3
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
    echo "It took $DURATION seconds to produce $NUM_MESSAGES messages"
    echo "Average productivity was $((NUM_MESSAGES * 60 / DURATION)) messages/minute"
    exit 0
}

rm -rf $PIPE
mkfifo $PIPE

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

escribe_a_pipe & write_to_kafka & read

