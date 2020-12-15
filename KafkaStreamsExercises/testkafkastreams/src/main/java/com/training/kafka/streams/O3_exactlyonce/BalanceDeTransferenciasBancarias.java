package com.training.kafka.streams.O3_exactlyonce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BalanceDeTransferenciasBancarias {
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

    /*
        Así creamos los tópicos que necesita la aplicación DESDE MI WINDOWS

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic bank-transactions

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic bank-balance-exactly-once

    Los mensajes se producen ejecutando el main() de ProductorDeTransferenciasBancarias.java

    Así creamos los tópicos en LINUX

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
    --replication-factor 1 --partitions 3 --topic bank-transactions

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
    --replication-factor 1 --partitions 3 --topic bank-balance-exactly-once

    Los mensajes se producen ejecutando el main() de ProductorDeTransferenciasBancarias.java

 */
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Si el broker estuviese Kerberizado usaríamos esta configuración
        // config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.12.61:9093");
        // config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.truststore.jks");
        // config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.keystore.jks");
        // config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");

        // config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Por defecto 3 seconds o más. Reducimos el tiempo para obener respuesta inmediata en kafka-console-consumer
        // No hacer este cambio en producción
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        // Poniendo este parámetro a 0 deshabilitamos la cache, de este modo el log mostrará todos los pasos
        // ya que se hará commit inmediatamente tras cada mensaje recibido. No aplicar este valor en producción.
        // Si no se cambia este valor, por defecto se hará un commit cada 30 segundos
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // KStreams no proporciona un JSON Serde, por lo que hemos de crearlo nosotros mismos
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream de Kafka: Leemos el stream de entrada como un KStream
        // <"Arturo". '{"nombre":"Arturo","cantidad":1271229733,"instante":"2018-12-02T07:20:37.550Z"}'>
        // <"Emilia". '{"nombre":"Emilia","cantidad":1271229733,"instante":"2018-12-02T07:20:37.550Z"}'>
        // <"Antofagasta". '{"nombre":"Antofagasta","cantidad":1271229733,"instante":"2018-12-02T07:20:37.550Z"}'>

        //    ...
        // 2. agrupamos por clave con GroupByKey, ya que ProductorDeTransferenciasBancarias
        // ya está produciendo los mesajes con la clave correcta, el campo "nombre" del mensaje JSON
        // 3. Aggegate, para calcular el balance bancario a partir del campo "cantidad"
        // 4. To: para escribir de nuevo a Kafka

        // TOPOLOGIA:

        // 1. Leer un sstream de un tópico de Kafka como un KStream de tuplas <String, String>
        //Serdes.String(), jsonSerde,

        KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions",
                Consumed.with(Serdes.String(), jsonSerde));

        // crear el objeto inicial json para los balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("numerotransferencia", 0);
        initialBalance.put("saldoencuenta", 0);
        initialBalance.put("instante", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                //  agrupamos por clave con GroupByKey
                // <"Arturo", '{"nombre":"Arturo","cantidad":1271229733,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Arturo", '{"nombre":"Arturo","cantidad":23423,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Arturo", '{"nombre":"Arturo","cantidad":3453,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Emilia", '{"nombre":"Emilia","cantidad":3453332,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Emilia", '{"nombre":"Emilia","cantidad":34534,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Antofagasta", '{"nombre":"Antofagasta","cantidad":2342,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Antofagasta", '{"nombre":"Antofagasta","cantidad":22342,"instante":"2018-12-02T07:20:37.550Z"}'>
                // <"Antofagasta", '{"nombre":"Antofagasta","cantidad":456424,"instante":"2018-12-02T07:20:37.550Z"}'>
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde));

        /*
        Así consumimos el balance EN WINDOWS:

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-consumer.bat \
                --bootstrap-server 10.0.12.61:9092 \
                --topic bank-balance-exactly-once   \
                --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     \
                --property print.key=true     --property print.value=true     \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
                --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

        Así consumimos el balance EN LINUX:

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-consumer.sh \
                --bootstrap-server 10.0.12.61:9092 \
                --topic bank-balance-exactly-once     \
                --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     \
                --property print.key=true     --property print.value=true     \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
                --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
         */
        Topology streamTopology = builder.build();

        KafkaStreams streams = new KafkaStreams(streamTopology, config);
        // Solo para desarrollo, no usar en producción
        streams.cleanUp();
        streams.start();

        // Mostramos las metricas del KStream
        //System.out.println(streams.metrics());

        // Mostramos la topologia del Stream de Kafka
        System.out.println(streamTopology.describe());
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("numerotransferencia", balance.get("numerotransferencia").asInt() + 1);
        newBalance.put("saldoencuenta", balance.get("saldoencuenta").asInt() + transaction.get("cantidad").asInt());

        Long balanceEpoch = Instant.parse(balance.get("instante").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("instante").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("instante", newBalanceInstant.toString());
        return newBalance;
    }

    /*

    Así borramos los tópicos DESDE WINDOWS:

    atarin.mistralbs@PORTATIL-192~ $
    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-transactions

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-balance-application-bank-balance-agg-changelog

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-balance-application-bank-balance-agg-changelog

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-balance-exactly-once

    Así borramos los tópicos desde LINUX:

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-transactions

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic bank-balance-application-bank-balance-agg-changelog

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topicbank-balance-application-bank-balance-agg-changelog

     */
}
