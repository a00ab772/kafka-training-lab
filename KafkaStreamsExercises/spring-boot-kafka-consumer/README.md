# Spring Boot with Kafka Consumer Example

This Project covers how to use Spring Boot with Spring Kafka to Consume JSON/String message from Kafka topics
## Start Zookeeper
- `bin/zookeeper-server-start.sh config/zookeeper.properties`

## Start Kafka Server
- `bin/kafka-server-start.sh config/server.properties`

## Create Kafka Topic
```administrador@formacionkafka:~
   07:34:19 $ /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic kafka-spring-cloud --partitions 1 --replication-factor 1
   Created topic "kafka-spring-cloud".

   administrador@formacionkafka:~
   07:34:50 $ /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic kafka-spring-cloud-json --partitions 1 --replication-factor 1
   Created topic "kafka-spring-cloud-json".
```

## Publish to the Kafka Topic via Console
- `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafka-spring-cloud`
- `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafka-spring-cloud-json`
