package goldencars.producers

import java.util.Properties

import goldencars.typesafeconfig.TypeSafeConfig
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.io.Source
import scala.util.Random

/*

administrador@kafkabroker1:~$ cat JSONMensaje1250SAR.json | /opt/kafka/bin/kafka-console-producer.sh
--broker-list 10.0.12.62:9092,10.0.12.52:9092,10.0.12.63:9092 --topic sar-req-res


administrador@kafkabroker1:~$ ^C
administrador@kafkabroker1:~$ /opt/kafka/bin/kafka-console-consumer.sh
--bootstrap-server 10.0.12.62:9092,10.0.12.52:9092,10.0.12.63:9092 --topic sar-req-res --from-beginning



 */
object KafkaMessagesProducer extends App {
  // Typesafe config
  val typesafeconf = TypeSafeConfig.Variables

  val jsonSAR = Source.fromInputStream(getClass.getResourceAsStream("/JSONMensaje1250SAR.json")).mkString

  println(jsonSAR)

  val rnd = new Random()

  val topic = typesafeconf.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, typesafeconf.kafkaBroker)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "SARProducer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  for (fileCount <- 1 to typesafeconf.numberOfFiles) {
    //val fw = new FileWriter(filePath, true)

    // introduce some randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(typesafeconf.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to typesafeconf.records) {
      val producerRecord = new ProducerRecord(topic, jsonSAR)
      kafkaProducer.send(producerRecord)
      //fw.write(line)
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }

    val sleeping = 2000
    println(s"Sleeping for $sleeping ms")
  }

  kafkaProducer.close()
}