#!/bin/bash

kafka-topics --create --topic demo-1-standalone \
  --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-1-standalone --zookeeper 127.0.0.1:2181

# El primer parámetro ha de ser la configuración del Worker
# Los siguientes parámetros pueden ser tantos conectores como queramos ejecutar en el
# Kafka Connect standalone cluster
connect-standalone -daemon /tutorial/source/demo-1/worker.properties \
  /tutorial/source/demo-1/file-stream-demo-standalone.properties


echo -e "log linea 1\nlog linea 2\nlog linea 3" >> /tmp/demo-file.txt
echo -e "log linea 4\nlog linea 5\nlog linea 6" >> /tmp/demo-file.txt

kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-1-standalone --from-beginning

exit 0
