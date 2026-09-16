#!/bin/bash

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

$CONFLUENT_HOME/bin/kafka-console-consumer \
    --property 'print.timestamp=true' --property 'print.key=true' \
    --property 'print.offset=true' --property 'enable.auto.commit=false' \
    --bootstrap-server localhost:9092 --topic monitoring --group GroupA --from-beginning

