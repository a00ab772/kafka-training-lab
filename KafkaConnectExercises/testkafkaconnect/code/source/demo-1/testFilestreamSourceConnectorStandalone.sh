#!/bin/bash

kafka-topics --create --topic demo-1-standalone \
  --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-1-standalone --zookeeper 127.0.0.1:2181

# First parameter must be the worker configuration
# Next parameters can be as many as connectors we want to run
# in our Kafka Connect standalone cluster
connect-standalone -daemon /tutorial/source/demo-1/worker.properties \
  /tutorial/source/demo-1/file-stream-demo-standalone.properties

sleep 15

echo -e "log line 1\nlog line 2\nlog line 3" >> /tmp/demo-file.txt
echo -e "log line 4\nlog line 5\nlog line 6" >> /tmp/demo-file.txt

kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-1-standalone --from-beginning

exit 0
