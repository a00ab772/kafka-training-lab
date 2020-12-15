package com.training.kafka.batch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class ConsumerSSL {
    public static void main(String[] args){

        // Configuración del consumidor de Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        // Si el broker estuviese Kerberizado usaríamos esta configuración
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/keytabs/ssl_client/kafka.client.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/client.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "clientpass");
        //props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/keytabs/ssl_client/kafka.client.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka.client.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "clientpass");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "clientpass");


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
            System.out.println("Todavía no tengo asignadas particiones...");
        }
        else {
            System.out.println("Tengo asignadas estas particiones:");
            for (TopicPartition partition: collection){
                System.out.println(String.format("Partición: %s en tópico: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Tópico: %s, Partición: %d, Offset: %d, Clave: %s, Valor: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
