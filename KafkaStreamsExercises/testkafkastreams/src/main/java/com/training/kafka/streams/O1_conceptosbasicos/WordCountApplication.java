package com.training.kafka.streams.O1_conceptosbasicos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class WordCountApplication {
    public static void main(String[] args) {
        /* *
        OJO: SI USAS WINDOWS DEBES DE TENER LA ENTRADA DE TU LABORATORIO EN EL C:\Windows\system32\drivers\etc\hosts

        omo ves aquí, yo tengo el mío

        atarin.mistralbs@PORTATIL-192~$ cat /c/Windows/System32/drivers/etc/hosts

        10.0.12.52    kafka_test

        172.29.72.100 alckafka01
        172.29.72.101 alckafka02
        172.29.72.102 alckafka03

        10.0.12.61    formacionkafka

         */
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Si el broker estuviese Kerberizado usaríamos esta configuración
        // config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.12.61:9093");
        // config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.truststore.jks");
        // config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.keystore.jks");
        // config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");
	
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.POLL_MS_CONFIG, 500);
        // Por defecto 3 segundos o más. Reducimos el tiempo para obener respuesta inmediata en kafka-console-consumer
        // No hacer este cambio en producción
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsBuilder builder = new StreamsBuilder();

        /*
        Así creamos los tópicos que necesita la aplicación DESDE MI WINDOWS

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic streams-plaintext-input

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic streams-wordcount-output

        Así se producen los mensajes DESDE WINDOWS:

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-producer.bat \
            --broker-list 10.0.12.61:9092 --topic streams-plaintext-input

        Así creamos los tópicos en LINUX

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic streams-plaintext-input

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic streams-wordcount-output

        Así se producen los mensajes DESDE LINUX:

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-producer.sh \
                --broker-list 10.0.12.61:9092 --topic streams-plaintext-input

        Así se produce desde la PROPIA MÁQUINA DEL LABORATORIO:

        administrador@formacionkafka:~/Ejercicios $
        /opt/kafka/kafka/bin/kafka-console-producer.sh --broker-list formacionkafka:9092 \
        --topic streams-plaintext-input

         */
        // 1. Stream de entrada de Kafka:
        //    <null, "Kafka Kafka Streams">
        KStream<String, String> wordCountInput = builder.stream("streams-plaintext-input");

        // 2. MapValues para convertir el texto a a minusculas:
        //    <null, "kafka kafka streams">
        KTable<String, Long> wordCounts =
                wordCountInput.mapValues(value -> value.toLowerCase())
                // 3. FlatMapValues para separar por espacio:
                //    <null, "kafka">, <null, "kafka">, <null, "streams">
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4. SelectKey para aplicar una clave, en vez de la nula que viene por defecto:
                //    <"kafka", "kafka">, <"kafka", "kafka">, <"streams", "streams">
                .selectKey((ignoredKey, word) -> word)
                // 5. GroupByKey antes de contar, agrupamos:
                //    (<"kafka", "kafka">, <"kafka", "kafka">) , (<"streams", "streams">)
                .groupByKey()
                // 6. Contamos las ocurrencias:
                //    <"kafka", 2>, <"streams", 1>
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("Counts"));

        // 7. To:
        // escribimos los resultados a Kafka
        wordCounts.toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        /*
        Así consumimos LA SALIDA DESDE WINDOWS:

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-consumer.bat \
                --bootstrap-server 10.0.12.61:9092 \
                --topic streams-wordcount-output     \
                --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     \
                --property print.key=true     --property print.value=true     \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
                --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

        Se consumen así desde línea de comando de LINUX:

         ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-consumer.sh \
         --bootstrap-server 10.0.12.61:9092 --topic streams-wordcount-output   \
         --from-beginning     --formatter kafka.tools.DefaultMessageFormatter \
         --property print.key=true     --property print.value=true              \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
         */

        Topology streamTopology = builder.build();

        KafkaStreams streams = new KafkaStreams(streamTopology, config);

        streams.start();

        // Este comando escribe las metricas de la aplicacion
        //System.out.println(streams.metrics());
        // Este comando escribe la topologa de tu Kafka Stream
        System.out.println(streamTopology.describe());
        System.out.println(streams.toString());

        // Añadimos el "shutdown hook", para detener ordenadamente los hilos de Kafka Streams
        // Se puede añadir un timeout si hiciese falta
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /*

    Así borramos los tópicos DESDE WINDOWS:

    atarin.mistralbs@PORTATIL-192~ $
    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-plaintext-input

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-starter-app-Counts-changelog

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-starter-app-Counts-repartition

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-wordcount-output

    Así borramos los tópicos DESDE LINUX:

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-plaintext-input

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic  streams-starter-app-Counts-changelog

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-starter-app-Counts-repartition

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic streams-wordcount-output
     */
}
