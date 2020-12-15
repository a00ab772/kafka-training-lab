package com.training.kafka.streams.O4_joins;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MaestroDetalleTransaccionStateful {
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
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
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

        StreamsBuilder builder = new StreamsBuilder();

        /*
        Así creamos los tópicos que necesita la aplicación DESDE MI WINDOWS

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic user-table

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic user-purchases

        O desde mi LINUX:

         ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic user-table

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic user-purchases

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic user-purchases-enriched-left-join

        Los mensajes se producen ejecutando el main() de GeneradorTransacciones.java

     */


        // Creamos una tabla global a partir de un tópico de Kafka.
        // Esta tabla se replicará en cada aplicación Kafka Streams que arranquemos
        // la clave de la globalKTable será el ID de usuario
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

        // Leemos el stream de compras de usuario
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        // Enriquecemos el stream de compras con los atributos del usuario mediante un join
        KStream<String, String> userPurchasesEnrichedJoin =
                userPurchases.join(usersGlobalTable,
                        (key, value) -> key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
                );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // Enriquecemos el stream con Left Join
        KStream<String, String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(usersGlobalTable,
                        (key, value) -> key, /* map de (key, value) del stream a la key de la GlobalKTable */
                        (userPurchase, userInfo) -> {
                            // Al ser left join, userInfo puede ser null
                            if (userInfo != null) {
                                return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                            } else {
                                return "Purchase=" + userPurchase + ",UserInfo=null";
                            }
                        }
                );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

                /*
        Así consumimos el contenido de la tabla persistente DESDE WINDOWS:

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-consumer.bat \
                --bootstrap-server 10.0.12.61:9092 \
                --topic user-purchases-enriched-left-join     \
                --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     \
                --property print.key=true     --property print.value=true     \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
                --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

        DESDE LINUX:

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-consumer.sh \
                --bootstrap-server 10.0.12.61:9092 \
                --topic user-purchases-enriched-left-join     \
                --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     \
                --property print.key=true     --property print.value=true     \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
                --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
         */

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp(); // No hacer esto en producción
        streams.start();

        // Imprime la topologia de la transformación del stream
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    /*
    Así borramos los tópicos DESDE WINDOWS:

    atarin.mistralbs@PORTATIL-192~ $
    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic user-table

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases-enriched-left-join

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases-enriched-inner-join

    DESDE LINUX:

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic user-table

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases-enriched-left-join

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic user-purchases-enriched-inner-join

     */
}
