package com.training.kafka.streams.O3_exactlyonce;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

    /*
    Así creamos los tópicos
    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
    --replication-factor 1 --partitions 3 --topic bank-transactions

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
    --replication-factor 1 --partitions 3 --topic bank-balance-exactly-once

    Los mensajes se producen ejecutando el main() de ProductorDeTransferenciasBancarias.java

 */

public class ProductorDeTransferenciasBancarias {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("juan"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("perico"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("andres"));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        // creates an empty json {}
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        // { "cantidad" : 46 } (46 is a random number between 0 and 100 excluded)
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

        // Instant.now() is to get the current time using Java 8
        Instant now = Instant.now();

        // we write the data to the json document
        transaction.put("nombre", name);
        transaction.put("cantidad", amount);
        transaction.put("instante", now.toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }

    /*
    Así borramos los tópicos:

    arturotarin@QOSMIO-X70B:~
    09:21:52 $ ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-transactions

    09:21:52 $ ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-balance-exactly-once
     */

}
