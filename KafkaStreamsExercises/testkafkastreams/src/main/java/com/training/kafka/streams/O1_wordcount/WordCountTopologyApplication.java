package com.training.kafka.streams.O1_wordcount;

import com.fasterxml.jackson.databind.util.Named;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountTopologyApplication {
    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Uncomment this block if you want to use SSL authorization and encryption
        // config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        // config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/kafka.client.truststore.jks");
        // config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka.client.keystore.jks");
        // config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");
	
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.POLL_MS_CONFIG, 500);
        // This parameter is 3 seconds by default. We are reducing it here to get an immediate
        // output in the kafka-console-consumer command line, for educational purposes.
        //it is absolutely not recommended making this change in production environments
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsBuilder builder = new StreamsBuilder();

        /*
         Produce messages to Kafka from command line

         export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

         $CONFLUENT_HOME/bin/kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
         > Kafka Kafka Streams

         */
        // 1. Input stream example:
        //    <null, "Kafka Kafka Streams">
        KStream<String, String> wordCountInput = builder.stream("streams-plaintext-input");

        wordCountInput.print(Printed.toSysOut());

        // 2. MapValues: converting input records text to lowercase:
        //    <null, "kafka kafka streams">
        KTable<String, Long> wordCounts =
                wordCountInput.mapValues(value -> value.toLowerCase())
                // 3. FlatMapValues split input records by " ":
                //    <null, "kafka">, <null, "kafka">, <null, "streams">
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4. SelectKey record keys will now change from their current null to the value of the record:
                //    <"kafka", "kafka">, <"kafka", "kafka">, <"streams", "streams">
                .selectKey((ignoredKey, word) -> word)
                // 5. GroupByKey: grouping by key before counting keys:
                //    (<"kafka", "kafka">, <"kafka", "kafka">) , (<"streams", "streams">)
                .groupByKey()
                // 6. Count by key:
                //    <"kafka", 2>, <"streams", 1>
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("Counts"));

        // 7. To:
        // write back to a Kafka topic
        wordCounts.toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
        wordCounts.toStream().print(Printed.toSysOut());

        /*
        Consume results from a Linux command line:

         export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

         $CONFLUENT_HOME/bin/kafka-console-consumer \
         --bootstrap-server localhost:9092 --topic streams-wordcount-output   \
         --from-beginning     --formatter kafka.tools.DefaultMessageFormatter \
         --property print.key=true     --property print.value=true              \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
        kafka	2
        streams	1

        Kafka has created two additional topics in order to support the transformations applied in the
        Topology steps:

         $ $CONFLUENT_HOME/bin/kafka-topics --zookeeper localhost:2181 --list | grep app
        streams-starter-app-Counts-changelog
        streams-starter-app-Counts-repartition
         */

        Topology streamTopology = builder.build();

        KafkaStreams streams = new KafkaStreams(streamTopology, config);

        streams.start();

        // This command writes application metrics
        streams.metrics().values().forEach(value -> {
            System.out.println(value.metricName() + "=" + value.metricValue());
        });

        // This command prints the Kafka Streams topology
        System.out.println("\n||||||||||||||||||\n" + streamTopology.describe() +
                        "\n||||||||||||||||||\n"+
                "You can see the graph if you paste the output here https://zz85.github.io/kafka-streams-viz/\n");

        // Add "shutdown hook" so that Kafka Streams can stop the process threads in an organized way
        // If necessary we can add a timeout
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
