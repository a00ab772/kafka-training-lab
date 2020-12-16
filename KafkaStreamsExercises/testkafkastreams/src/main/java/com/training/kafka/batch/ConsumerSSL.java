package com.training.kafka.batch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class ConsumerSSL {
    public static void main(String[] args){

        // Kafka consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");

         props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
         props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/kafka.client.truststore.jks");
         props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
         props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka.client.keystore.jks");
         props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
         props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.min.bytes", 1);
        props.put("group.id", "GrupoA");
        props.put("heartbeat.interval.ms", 3000);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "latest");
        props.put("connections.max.idle.ms", 540000);
        props.put("enable.auto.commit", true);
        props.put("exclude.internal.topics", true);
        props.put("max.poll.records", 2147483647);
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("request.timeout.ms", 40000);
        props.put("auto.commit.interval.ms", 5000);
        props.put("fetch.max.wait.ms", 500);
        props.put("metadata.max.age.ms", 300000);
        props.put("reconnect.backoff.ms", 50);
        props.put("retry.backoff.ms", 100);
        props.put("client.id", "");

        String topico = "producer-consumer";

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(props);

        consumidor.subscribe(Arrays.asList(topico));

        // Continuously read records coming in
        try {
            while (true){
                ConsumerRecords records = consumidor.poll(1000);
                printRecords(records);
            }
        } finally {
            consumidor.close();
        }

    }


    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
