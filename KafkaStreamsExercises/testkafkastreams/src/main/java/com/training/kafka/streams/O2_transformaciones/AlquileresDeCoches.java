package com.training.kafka.streams.O2_transformaciones;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class AlquileresDeCoches {
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

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "alquiler-coches");
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
	
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Por defecto 3 segundos o más. Reducimos el tiempo para obener respuesta inmediata en kafka-console-consumer
        // No hacer este cambio en producción
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        // Deshabilitada la cache para mostrar todos los pasos. No hacerlo en producción
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        /*
        Así creamos los tópicos que necesita la aplicación DESDE MI WINDOWS

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic alquiler-coches-entrada

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic clientes-y-coches

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --create --zookeeper 10.0.12.61:2181 \
                 --replication-factor 1 --partitions 3 --topic alquiler-coches-salida


        Así se producen los mensajes en WINDOWS:

        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-producer.bat \
            --broker-list 10.0.12.61:9092 --topic alquiler-coches-entrada



        Así creamos los tópicos que necesita la aplicación en LINUX

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic alquiler-coches-entrada

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic clientes-y-coches

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --create --zookeeper 10.0.12.61:2181 \
        --replication-factor 1 --partitions 3 --topic alquiler-coches-salida

        Así se producen los mensajes en LINUX:

        ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-producer.sh \
            --broker-list 10.0.12.61:9092 --topic alquiler-coches-entrada
        Recaredo,cocheverde
        Recaredo,cocheverde
        Recaredo,cocheverde
        Arturo,cocheverde
        Recaredo,cocheverde
        Ernesto,cocherojo
        Juana,cocheazul
        Vicenta,cochedecolorinexistente
        EsteClienteNOSEPARACONCOMAcocherojo


        Así se produce desde la PROPIA MÁQUINA DEL LABORATORIO:

        administrador@formacionkafka:~/Ejercicios $
        /opt/kafka/kafka/bin/kafka-console-producer.sh --broker-list formacionkafka:9092 \
        --topic alquiler-coches-entrada
        Recaredo,cocheverde
        Recaredo,cocheverde
        Recaredo,cocheverde
        Arturo,cocheverde
        Recaredo,cocheverde
        Ernesto,cocherojo
        Juana,cocheazul
        Vicenta,cochedecolorinexistente
        EsteClienteNOSEPARACONCOMAcocherojo

         */

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream de Kafka: la entrada no tiene claves
        //    <null, "Recaredo,cocheverde">
        //    <null, "Arturo,cochemorado">
        //    <null, "Emi,cochemorado">
        //    <null, "Recaredo,cocheazul">
        //    ...
        // Leemos el stream de entrada como un KStream y le extraemos el coche alquilado como clave.
        // A continuación escribimos el resultado en Kafka como un topico de log compactado

        // 2. Los resultados podrán entonces ser leídos como una KTable, de modo que podremos hacer actualizaciones
        // 3. A continuación agruparemos los registros de la KTable, pirmero con groupBy y luego con count
        // 4. Por último escribiremos los resultados en un tópico de salida de Kafka

        // TOPOLOGIA:

        // 1. Leer un sstream de un tópico de Kafka como un KStream de tuplas <String, String>
        KStream<String, String> registro = builder.stream("alquiler-coches-entrada");

        KStream<String, String> clientesYcochesAlquilados = registro
                // a. filtramos las líneas de entrada para eliminar los valores incorrectos.
                // Los valores correctos llevarán una ',' entre el nombre del cliente y el coche que alquila
                // <null, "cliente1,cocherojo">
                // <null, "cliente2,cocherojo">
                // <null, "cliente3,cocherojo">
                // <null, "cliente4,cocheazul">
                // <null, "cliente5,cocheazul">
                // <null, "clienteERRATAcocheazul">
                // <null, "cliente7,cocheOTROCOLOR">
                .filter((clave, valor) -> valor.contains(","))
                // b. Seleccionamos la clave:
                // <"cliente1", "cliente1,cocherojo">
                // <"cliente2", "cliente2,cocherojo">
                // <"cliente3", "cliente3,cocherojo">
                // <"cliente4", "cliente4,cocheazul">
                // <"cliente5", "cliente5,cocheazul">
                // <"cliente7", "cliente7,cocheOTROCOLOR">
                .selectKey((clave, valor) -> valor.split(",")[0].toLowerCase())
                // c. seleccionamos el coche
                // <"cocherojo">
                // <"cocherojo">
                // <"cocherojo">
                // <"cocheazul">
                // <"cocheazul">
                // <"cocheOTROCOLOR">
                .mapValues(valor -> valor.split(",")[1].toLowerCase())
                // d. filtramos para contar unos cuantos tipos de coches
                .filter((cliente, coche) -> Arrays.asList("cocherojo", "cocheverde", "cocheazul").contains(coche));

        // 2. Escribimos a un tópico temporal de Kafka, que nos permitirá agrupar por claves
        // <"cocherojo">
        // <"cocherojo">
        // <"cocherojo">
        // <"cocheazul">
        // <"cocheazul">
        clientesYcochesAlquilados.to("clientes-y-coches");

        // 3. Leemos el tópico intermedio de Kafka como una KTable
        KTable<String, String> tablaClientesYCoches = builder.table("clientes-y-coches");

        // 4. Contamos los alquileres que han habido ordenando por coche
        KTable<String, Long> alquileresDeCoches = tablaClientesYCoches
                // a. agrupar por coche en la tabla
                // <"cocherojo", "cocherojo">
                // <"cocherojo", "cocherojo">
                // <"cocherojo", "cocherojo">

                // <"cocheazul", "cocheazul">
                // <"cocheazul", "cocheazul">
                .groupBy((cliente, coche) -> new KeyValue<>(coche, coche))
                // b. contamos las ocurrencias por cada coche y retornams el resultado en una KTable
                // <"cocherojo", "3">
                // <"cocheazul", "2">
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CuentaCochesAlquilados"));
        // 5. Escribir el resultado en el tópico de salida de Kafka
        alquileresDeCoches.toStream().to("alquiler-coches-salida", Produced.with(Serdes.String(), Serdes.Long()));


        /*

         En entornos WINDOWS Se consumen desde línea de comando de este modo:

        atarin.mistralbs@PORTATIL-192~ $
        ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-console-consumer.bat \
         --bootstrap-server 10.0.12.61:9092 --topic alquiler-coches-salida   \
         --from-beginning     --formatter kafka.tools.DefaultMessageFormatter \
         --property print.key=true     --property print.value=true              \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

         En entornos LINUX Se consumen desde línea de comando de este modo:

         ~/Downloads/kafka_2.12-2.0.0/bin/kafka-console-consumer.sh \
         --bootstrap-server 10.0.12.61:9092 --topic alquiler-coches-salida   \
         --from-beginning     --formatter kafka.tools.DefaultMessageFormatter \
         --property print.key=true     --property print.value=true              \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     \
          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
            cocheverde	1
            cocheverde	0
            cocheverde	1
            cocheverde	0
            cocheverde	1
            cocheverde	2
            cocheverde	1
            cocheverde	2
            cocheazul	1
            cocheazul	2

         */


        Topology streamTopology = builder.build();

        KafkaStreams streams = new KafkaStreams(streamTopology, config);
        // Solo en desarrollo, no en producción
        streams.cleanUp();

        streams.start();

        // Mostramos la topologia del Stream de Kafka
        //System.out.println(streams.metrics());
        System.out.println(streamTopology.describe());

        // "shutdown hook" para detener ordenadamente los hilos de Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    /*
    Borramos los tópicos al acabar la prueba EN WINDOWS:


    atarin.mistralbs@PORTATIL-192~ $
    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-entrada

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic clientes-y-coches

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-salida

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-CuentaCochesAlquilados-changelog

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-CuentaCochesAlquilados-repartition

    ~/Documents/kafka_2.12-1.1.0/bin/windows/kafka-topics.bat --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-clientes-y-coches-STATE-STORE-0000000006-changelog

    Borramos los tópicos al acabar la prueba EN LINUX:

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-entrada

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic clientes-y-coches

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-salida

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-CuentaCochesAlquilados-changelog

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-CuentaCochesAlquilados-repartition

    ~/Downloads/kafka_2.12-2.0.0/bin/kafka-topics.sh --zookeeper 10.0.12.61:2181 \
    --delete --topic alquiler-coches-clientes-y-coches-STATE-STORE-0000000006-changelog

     */
}
