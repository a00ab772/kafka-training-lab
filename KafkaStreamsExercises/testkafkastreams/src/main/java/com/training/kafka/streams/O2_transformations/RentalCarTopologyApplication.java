package com.training.kafka.streams.O2_transformations;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class RentalCarTopologyApplication {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rental-car-input");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Uncomment the following lines if you want to use SSL authentication and encryption
        // config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        // config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/kafka.client.truststore.jks");
        // config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka.client.keystore.jks");
        // config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        // config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");
	
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // This parameter is 3 seconds by default. We are reducing it here to get an immediate
        // output in the kafka-console-consumer command line, for educational purposes.
        //it is absolutely not recommended making this change in production environments
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        // Cache disabled for education purposes, in order to see all the records.
        // Don't do it in Production environments
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

/*
         Produce messages to Kafka from command line

         export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

         $CONFLUENT_HOME/bin/kafka-console-producer --broker-list localhost:9092 --topic  rental-car-input
         > Recaredo,greencar
         > Recaredo,bluecar
         > Recaredo,purplecar
         > Arturo,greencar
         > Recaredo,greencar
         > Ernesto,redcar
         > Juana,bluecar
         > Recaredo,bluecar
         > Arturo,purplecar
         > Emi,purplecar
         > Vicenta,UNEXPECTEDCOLORcar
         > EsteClienteMISSINGSEPARATORredcar
         */

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Input Kafka stream: it has null keys
        // [KSTREAM-SOURCE-0000000000]: null, Recaredo,greencar
        // [KSTREAM-SOURCE-0000000000]: null, Recaredo,bluecar
        // [KSTREAM-SOURCE-0000000000]: null, Recaredo,purplecar
        // [KSTREAM-SOURCE-0000000000]: null, Arturo,greencar
        // [KSTREAM-SOURCE-0000000000]: null, Recaredo,greencar
        // [KSTREAM-SOURCE-0000000000]: null, Ernesto,redcar
        // [KSTREAM-SOURCE-0000000000]: null, Juana,bluecar
        // [KSTREAM-SOURCE-0000000000]: null, Recaredo,bluecar
        // [KSTREAM-SOURCE-0000000000]: null, Arturo,purplecar
        // [KSTREAM-SOURCE-0000000000]: null, Emi,purplecar
        // [KSTREAM-SOURCE-0000000000]: null, EsteClienteMISSINGSEPARATORredcar
        // [KSTREAM-SOURCE-0000000000]: null, Vicenta,UNEXPECTEDCOLORcar

        //    ...
        // We read the input stream as a KStream and extract the rented car as a key.
        // Then we write the result in Kafka as a compact log topic

        // 2. The results can then be read as a KTable, so we can make updates
        // 3. Next we will group the records of the KTable, first with groupBy and then with count
        // 4. Finally we will write the results in a Kafka's output topic

        // TOPOLOGY:

        // 1. Read a stream of a Kafka topic as a KStream of <String, String> tuples
        KStream<String, String> inputStream = builder.stream("rental-car-input");

        inputStream.print(Printed.toSysOut());

        KStream<String, String> customersAndColorsRentedStream = inputStream
                // [KSTREAM-SOURCE-0000000000]: null, Recaredo,greencar
                // [KSTREAM-SOURCE-0000000000]: null, Recaredo,bluecar
                // [KSTREAM-SOURCE-0000000000]: null, Recaredo,purplecar
                // [KSTREAM-SOURCE-0000000000]: null, Arturo,greencar
                // [KSTREAM-SOURCE-0000000000]: null, Recaredo,greencar
                // [KSTREAM-SOURCE-0000000000]: null, Ernesto,redcar
                // [KSTREAM-SOURCE-0000000000]: null, Juana,bluecar
                // [KSTREAM-SOURCE-0000000000]: null, Recaredo,bluecar
                // [KSTREAM-SOURCE-0000000000]: null, Arturo,purplecar
                // [KSTREAM-SOURCE-0000000000]: null, Emi,purplecar
                // [KSTREAM-SOURCE-0000000000]: null, EsteClienteMISSINGSEPARATORredcar
                // [KSTREAM-SOURCE-0000000000]: null, Vicenta,UNEXPECTEDCOLORcar

                // a. we filter the input lines to remove the incorrect values.
                // The correct values will have a ',' between the customer's name and the car you are renting
                .filter((clave, valor) -> valor.contains(","))
                // [KSTREAM-FILTER-0000000005]: recaredo, greencar
                // [KSTREAM-FILTER-0000000005]: recaredo, bluecar
                // [KSTREAM-FILTER-0000000005]: arturo, greencar
                // [KSTREAM-FILTER-0000000005]: recaredo, greencar
                // [KSTREAM-FILTER-0000000005]: ernesto, redcar
                // [KSTREAM-FILTER-0000000005]: juana, bluecar
                // [KSTREAM-FILTER-0000000005]: recaredo, bluecar
                // [KSTREAM-FILTER-0000000005]: arturo,purplecar
                // [KSTREAM-FILTER-0000000005]: emi,purplecar
                // [KSTREAM-FILTER-00000000050]: vicenta,UNEXPECTEDCOLORcar

                // b.Assign the customer name as the key for the tuple that is going to be sent downstream:
                .selectKey((clave, valor) -> valor.split(",")[0].toLowerCase())
                // <"recaredo", "recaredo, greencar">
                // <"recaredo", "recaredo, bluecar">
                // <"arturo", "arturo, greencar">
                // <"recaredo", "recaredo, greencar">
                // <"ernesto", "ernesto, redcar">
                // <"juana", "juana, bluecar">
                // <"recaredo", "recaredo, bluecar">
                // <"arturo", "arturo, purplecar">
                // <"emi", "emi, purplecar">
                // <"vicenta", "vicenta, UNEXPECTEDCOLORcar">

                // c. funnel to only send the color value downstream
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // <"recaredo", "greencar">
                // <"recaredo", "bluecar">
                // <"arturo", "greencar">
                // <"recaredo", "greencar">
                // <"ernesto", "redcar">
                // <"juana", "bluecar">
                // <"recaredo", "bluecar">
                // <"arturo", "purplecar">
                // <"emi", "purplecar">
                // <"vicenta", "UNEXPECTEDCOLORcar">

                // d. we filter to count by available car colors
                .filter((customerName, carColor) -> Arrays.asList("redcar", "greencar", "bluecar").contains(carColor));
                // <"recaredo", "greencar">
                // <"recaredo", "bluecar">
                // <"arturo", "greencar">
                // <"recaredo", "greencar">
                // <"ernesto", "redcar">
                // <"juana", "bluecar">
                // <"recaredo", "bluecar">

        customersAndColorsRentedStream.print(Printed.toSysOut());
        // [KTABLE-TOSTREAM-0000000011]: recaredo, greencar
        // [KTABLE-TOSTREAM-0000000011]: recaredo, bluecar
        // [KTABLE-TOSTREAM-0000000011]: arturo, greencar
        // [KTABLE-TOSTREAM-0000000011]: recaredo, greencar
        // [KTABLE-TOSTREAM-0000000011]: ernesto, redcar
        // [KTABLE-TOSTREAM-0000000011]: juana, bluecar
        // [KTABLE-TOSTREAM-0000000011]: recaredo, bluecar

        // 2. We write to a temporary topic of Kafka, which will allow us to group by keys
        customersAndColorsRentedStream.to("customers-and-cars");

        // 3. We read Kafka's intermediate topic as a KTable
        KTable<String, String> customersAndCarsTable = builder.table("customers-and-cars");

        customersAndCarsTable.toStream().print(Printed.toSysOut());

        // 4. We count the rentals that have been ordered by car
        KTable<String, Long> rentalCarTable = customersAndCarsTable
                // a. Group table by car color
                .groupBy((cliente, coche) -> new KeyValue<>(coche, coche))
                // <greencar, greencar>
                // <greencar, greencar>
                // <greencar, greencar>

                // <redcar, redcar>

                // <bluecar, bluecar>
                // <bluecar, bluecar>
                // <bluecar, bluecar>


                // b. we count the occurrences for each car and return the result in a KTable
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("RentedCarsCount"));
                // <"redcar", "1">
                // <"bluecar", "3">
                // <"greencar", "3">

        // 5. Write the result in Kafka's output topic
        rentalCarTable.toStream().to("rental-car-output", Produced.with(Serdes.String(), Serdes.Long()));

        rentalCarTable.toStream().print(Printed.toSysOut());

        /*
        Consume results from a Linux command line:

         export CONFLUENT_HOME=~/Downloads/confluent-6.0.0/

         $CONFLUENT_HOME/bin/kafka-console-consumer \
         --bootstrap-server localhost:9092 --topic rental-car-output   \
         --from-beginning     --formatter kafka.tools.DefaultMessageFormatter \
         --property print.key=true     --property print.value=true              \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
        greencar	1
        greencar	0
        bluecar	1
        greencar	1
        greencar	2
        redcar	1
        bluecar	2
        greencar	3
        bluecar	3


         */


        Topology streamTopology = builder.build();

        KafkaStreams streams = new KafkaStreams(streamTopology, config);

        // Cleaning up the streams only for Development purposes, not in production
        // streams.cleanUp();

        streams.start();

        // This command prints the Kafka Streams topology
        System.out.println("\n||||||||||||||||||\n" + streamTopology.describe() +
                "\n||||||||||||||||||\n"+
                "You can see the graph if you paste the output here https://zz85.github.io/kafka-streams-viz/\n");

        // Add "shutdown hook" so that Kafka Streams can stop the process threads in an organized way
        // If necessary we can add a timeout
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
