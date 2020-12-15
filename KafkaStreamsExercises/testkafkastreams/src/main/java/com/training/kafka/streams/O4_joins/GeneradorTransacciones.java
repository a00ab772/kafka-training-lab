package com.training.kafka.streams.O4_joins;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;



public class GeneradorTransacciones {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Maxima garantia
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // Usamos la capacidad de idempotencia de Kafka >=0.11, asegurandonos de que el productor no producira duplicados
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        Producer<String, String> producer = new KafkaProducer<>(properties);


        // No uses .get() en produccion. Usalo en desarrollo para asegurarte viendolo en pantalla que las escrituras en
        // los topicos son secuenciales,
        // ES UN COMANDO BLOQUEANTE, POR LO TANTO BLOQUEARIAS EL STREAM SI LO USASES EN PRODUCCION


        // Probamos distintos escenarios en los que ilustramos el Join de Kafka Streams

        // 1 - Creamos un nuevo usuario y enviamos datos a Kafka
        System.out.println("\nEjemplo 1 - usuario nuevo\n");
        producer.send(userRecord("arturo", "First=Arturo,Last=Tarin,Email=arturo.tarin@gmail.com")).get();
        producer.send(purchaseRecord("arturo", "Ford Fiesta")).get();

        Thread.sleep(10000);

        // 2 - Recibimos una compra de un usuario que no existe en Kafka
        System.out.println("\nEjemplo 2 - usuario inexistente\n");
        producer.send(purchaseRecord("wenceslao", "Renault Megane")).get();

        Thread.sleep(10000);

        // 3 - actualizamos a "arturo" y enviamos una nueva transaccion
        System.out.println("\nEjemplo 3 - actualizar usuario\n");
        producer.send(userRecord("arturo", "First=Arturo,Last=TarinVillaescusa,Email=atarin.mistralbs@goldcar.com")).get();
        producer.send(purchaseRecord("arturo", "Lamborgini del modelo que sea")).get();

        Thread.sleep(10000);

        // 4 - enviamos una compra de un usuario que no existia inicialmente en Kafka, pero que posteriormente si que existe
        System.out.println("\nEjemplo 4 - usuario que inicialmente no existe, luego si que existira\n");
        producer.send(purchaseRecord("anaxagoras", "Renault 4")).get();
        producer.send(userRecord("anaxagoras", "First=Anaxagoras,Last=DeKlazomene,GitHub=Presocratico")).get();
        producer.send(purchaseRecord("anaxagoras", "Renault 4")).get();
        producer.send(userRecord("anaxagoras", null)).get(); // Lo borramos para limpiar el topico

        Thread.sleep(10000);

        // 5 - creamos un usuario pero se borra antes de haber hecho la compra
        System.out.println("\nEjemplo 5 - se crea el usuario pero se borra antes de cumplimentar el proceso de compra\n");
        producer.send(userRecord("torcuato", "First=Torcuato")).get();
        producer.send(userRecord("torcuato", null)).get(); // Usuario borrado
        producer.send(purchaseRecord("torcuato", "Carraco")).get();

        Thread.sleep(10000);

        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }


    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<>("user-purchases", key, value);
    }
}
