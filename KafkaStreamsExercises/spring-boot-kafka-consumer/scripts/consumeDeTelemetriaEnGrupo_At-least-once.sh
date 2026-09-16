#!/bin/bash

~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-consumer.sh --property 'print.timestamp=true' --property 'print.key=true' --property 'print.offset=true' --property 'enable.auto.commit=true' --bootstrap-server localhost:9092 --topic telemetria --group GrupoA --from-beginning

