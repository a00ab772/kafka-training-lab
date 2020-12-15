package com.training.kafka.batch;

import org.apache.kafka.clients.producer.*;

import java.text.*;
import java.util.*;

public class Producer {

    public static void main(String[] args){

        // Kafka producer configuration properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("client.id", "");
        props.put("linger.ms", 0);
        props.put("max.block.ms", 60000);
        props.put("max.request.size", 1048576);
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("request.timeout.ms", 30000);
        props.put("timeout.ms", 30000);
        props.put("max.in.flight.requests.per.connection", 5);
        props.put("retry.backoff.ms", 5);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
        String topic = "producer-consumer";

        try {
            int numBatch = 1;
            int i = 0;
            while (true) {
                do {
                    System.out.println(String.format("Batch: %s || %s sent to the '%s' topic", Integer.toString(numBatch), dtFormat.format(new Date()), topic));
                    producer.send(
                            new ProducerRecord<String, String>(topic,
                                    String.format("Batch: %s || %s", Integer.toString(numBatch), dtFormat.format(new Date())))
                    );
                    i++;
                } while (i < 10);
                i = 0;
                Thread.sleep(500);
                numBatch++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
