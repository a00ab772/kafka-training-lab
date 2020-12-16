# Spring Boot with Kafka Consumer Example

This Project covers how to use Spring Boot with Spring Kafka to Consume JSON/String message from Kafka topics

## Start Kafka

```bash
kafka-training-lab ~ $ ./startLocalConfluent.sh 
```

## Create Kafka Topic
```bash
kafka-training-lab ~ $ KafkaConsoleExercises/createTopics.sh
Created topic "kafka-spring-cloud".
```

## Consume from the Kafka Topic via Console

- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka-spring-cloud --from-beginning`

## Publish message via WebService
- `http://localhost:8982/kafka/publish/Arturo/IT/4249239`
- `http://localhost:8982/kafka/publish/Tarin/Abc/5000000`

