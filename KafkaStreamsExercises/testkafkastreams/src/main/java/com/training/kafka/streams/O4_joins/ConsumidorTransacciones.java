package com.training.kafka.streams.O4_joins;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class ConsumidorTransacciones {
    public static void main(String[] args){

        // Configuración del consumidor de Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
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

        // Por defecto 3 seconds o más. Reducimos el tiempo para obener respuesta inmediata en kafka-console-consumer
        // No hacer este cambio en producción
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        String topico = "user-purchases-enriched-left-join";

        // Creamos el consumidor
        KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(props);

        /*
        // Indicamos de qué particiones vamos a consumir
        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(new TopicPartition("producer-consumer", 0));
        partitions.add(new TopicPartition("producer-consumer", 1));
        partitions.add(new TopicPartition("producer-consumer", 2));
        consumidor.assign(partitions);

        // Mostramos en pantalla la lista de subscripciones del consumidor
        Set<TopicPartition> assignedPartitions = consumidor.assignment();
        printSet(assignedPartitions);
        */

        consumidor.subscribe(Arrays.asList(topico));

        // Bucle para leer registros
        try {
            while (true){
                ConsumerRecords records = consumidor.poll(1000);
                printRecords(records);
            }
        } finally {
            consumidor.close();
        }

    }

    private static void printSet(Set<TopicPartition> collection){
        if (collection.isEmpty()) {
            System.out.println("There aren't partitions assigned yet...");
        }
        else {
            System.out.println("These partitions have been assigned:");
            for (TopicPartition partition: collection){
                System.out.println(String.format("Partition: %s en tópico: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
