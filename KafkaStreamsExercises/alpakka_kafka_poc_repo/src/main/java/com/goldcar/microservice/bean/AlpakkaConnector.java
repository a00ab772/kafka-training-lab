package com.goldcar.microservice.bean;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.javadsl.Transactional;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import com.goldcar.microservice.config.AlpakkaConfig;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

public class AlpakkaConnector {

    private AlpakkaConfig alpakkaConfig;

    public AlpakkaConfig getAlpakkaConfig() {
        return alpakkaConfig;
    }

    protected final ActorSystem system = ActorSystem.create("goldcarmicroservices");

    final Config consumerConfig = system.settings().config().getConfig("akka.kafka.consumer");

    private String bootstrapServer;

    private String consumerGroupId;

    protected AlpakkaConnector() {
    }


    public AlpakkaConnector(AlpakkaConfig alpakkaConfig) {
        this.alpakkaConfig = alpakkaConfig;
        this.bootstrapServer = getAlpakkaConfig().getBrokerHost()+":"+ getAlpakkaConfig().getBrokerPort();
        this.consumerGroupId = getAlpakkaConfig().getConsumerGroupId();
    }

    public boolean kafkaIsHealthy() {
        final ConsumerSettings<String, byte[]> consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                        .withBootstrapServers(getAlpakkaConfig().getBrokerHost()+":"+ getAlpakkaConfig().getBrokerPort())
                        .withGroupId(getAlpakkaConfig().getConsumerGroupId())
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", consumerSettings.getProperty("bootstrap.servers")); // "kafka-0.kafka-hs.default.svc.cluster.local:9093"
            props.put("group.id",consumerSettings.getProperty("group.id")); // "group1"
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer simpleConsumer = new KafkaConsumer(props);
            simpleConsumer.listTopics();
        } catch (Exception e) {
            return false;
        }

        return true;
    }


    /*
    Transactions

    Kafka Transactions provide guarantees that messages processed in a consume-transform-produce workflow
    (consumed from a source topic, transformed, and produced to a destination topic)
    are processed exactly once or not at all.

    This is achieved through coordination between the Kafka consumer group coordinator, transaction coordinator,
    and the consumer and producer clients used in the user application.

    The Kafka producer marks messages that are consumed from the source topic as “committed” only once the
    transformed messages are successfully produced to the sink.

    For full details on how transactions are achieved in Kafka you may wish to review the
    Kafka Improvement Proposal KIP-98: Exactly Once Delivery and Transactional Messaging

    https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging

    and its associated design document.

    https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#heading=h.xq0ee1vnpz4o

    SO IN ESSENCE: Use this class if you want delivery guarantees. It will guarantee the "all-or-nothing" and the
    the "exactly-once" message delivery, and will give you the ability to run transactions along several microservices that
    are comunicating between them through Kafka topics.
     */

    /*

          transactionalSink: implements the Consume-Transform-Produce Workflow that we mentioned in the header

          Kafka transactions are handled transparently to the user.

          The Transactional.source will enforce that a consumer group id is specified and the Transactional.flow
          or Transactional.sink will enforce that a transactional.id is specified.

          All other Kafka consumer and producer properties required to enable transactions are overridden.

          Transactions are committed on an interval which can be controlled with the producer config akka.kafka.producer.eos-commit-interval,
          similar to how "exactly once" works with Kafka Streams. The default value is 100ms.

          The larger commit interval is, the more records will need to be reprocessed in the event
          of a failure and it's corresponding transaction abortion.

          When the stream is materialized the producer will initialize the transaction for the provided transactional.id
          and a transaction will begin.

          We check if there are any offsets available to commit in every commit interval (eos-commit-interval) .

          If offsets exist then we suspend backpressured demand while we drain all outstanding messages
          that have not yet been successfully acknowledged (if any), and then commit the transaction.

          After the commit succeeds a new transaction is begun and we re-initialize demand for upstream messages.

          To gracefully shutdown the stream and commit the current transaction you must call shutdown() on the Control
          (https://doc.akka.io/api/akka-stream-kafka/0.22/akka/kafka/javadsl/Consumer$$Control.html)
          materialized value to await all produced message acknowledgements and commit the final transaction.

           */
    public void transactionalSink(String sourceTopic, String sinkTopic, String transactionalId) {

        final ProducerSettings<String, byte[]> consumerToProducerSettings =
                ProducerSettings.create(system, new StringSerializer(), new ByteArraySerializer())
                        .withBootstrapServers(bootstrapServer);

        final ConsumerSettings<String, byte[]> consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                        .withBootstrapServers(bootstrapServer)
                        .withGroupId(consumerGroupId)
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer.Control control =
                Transactional
                        .source(consumerSettings, Subscriptions.topics(sourceTopic))
                        .via(business())
                        .map(msg ->
                                new ProducerMessage.Message<String, byte[], ConsumerMessage.PartitionOffset>(
                                        new ProducerRecord<>(sinkTopic, msg.record().value()), msg.partitionOffset()))
                        .to(Transactional.sink(consumerToProducerSettings, transactionalId))
                        .run(materializer);

        // ...

        control.shutdown();

    }


    /*

      transactionalFailureRetry: implements transactional recovery from a failure

      When any stage in the stream fails the whole stream will be torn down.

      In the general case it’s desirable to allow transient errors to fail the whole stream
      because they cannot be recovered from within the application.

      Transient errors can be caused by network partitions, Kafka broker failures,
      ProducerFencedException’s from other application instances, and so on.

      When the stream encounters transient errors then the current transaction will be aborted before
      the stream is torn down.

      Any produced messages that were not committed will not be available to downstream consumers
      as long as those consumers are configured with

      isolation.level = read_committed.

      For transient errors we can choose to rely on the Kafka producer’s alpakkaConfig to retry,
      or we can handle it ourselves at the Akka Streams or Application layer.

      Using the RestartSource (https://doc.akka.io/docs/akka/2.5.13/stream/stream-error.html#delayed-restarts-with-a-backoff-stage)
      we can backoff connection attempts so that we don’t hammer the Kafka cluster in a tight loop.

     */
    public void transactionalFailureRetry(String sourceTopic, String sinkTopic, String transactionalId) {

        final ProducerSettings<String, byte[]> consumerToProducerSettings =
                ProducerSettings.create(system, new StringSerializer(), new ByteArraySerializer())
                        .withBootstrapServers(bootstrapServer);

        final ConsumerSettings<String, byte[]> consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                        .withBootstrapServers(bootstrapServer)
                        .withGroupId(consumerGroupId)
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        AtomicReference<Consumer.Control> innerControl = null;

        Source<ProducerMessage.Results<String, byte[], ConsumerMessage.PartitionOffset>,NotUsed> stream =
                RestartSource.onFailuresWithBackoff(
                        java.time.Duration.of(3, ChronoUnit.SECONDS), // min backoff
                        java.time.Duration.of(30, ChronoUnit.SECONDS), // max backoff
                        0.2, // adds 20% "noise" to vary the intervals slightly
                        () -> Transactional.source(consumerSettings, Subscriptions.topics(sourceTopic))
                                .via(business())
                                .map(msg ->
                                        new ProducerMessage.Message<String, byte[], ConsumerMessage.PartitionOffset>(
                                                new ProducerRecord<>(sinkTopic, msg.record().value()), msg.partitionOffset()))
                                // side effect out the `Control` materialized value because it can't be propagated through
                                // the `RestartSource`
                                .mapMaterializedValue(control -> {
                                    innerControl.set(control);
                                    return control;
                                })
                                .via(Transactional.flow(consumerToProducerSettings, transactionalId)));

        stream.runWith(Sink.ignore(), materializer);

        // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
        Runtime.getRuntime().addShutdownHook(new Thread(() -> innerControl.get().shutdown()));
    }

    protected <T> Flow<T, T, NotUsed> business() {
        return Flow.create();
    }



    protected final Materializer materializer = ActorMaterializer.create(system);


    /*
        produceMessagesAsAFlow produces `numMessages` messages to the topic `topic` and allows the stream to continue
        after publishing messages to Kafka.

        It accepts implementations of ProducerMessage.Envelope (API) as input, which continue in the flow as implementations
        of ProducerMessage.Results (API).
        */
    public void produceMessagesAsAFlow(int numMessages, String topic) {

        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers(bootstrapServer);

        // #flow
        CompletionStage<Done> done =
                Source.range(1, numMessages)
                        .map(number -> {
                            int partition = 0;
                            String value = String.valueOf(number);
                            ProducerMessage.Envelope<String, String, Integer> msg =
                                    new ProducerMessage.Message<String, String, Integer>(
                                            new ProducerRecord<>(topic, partition, "key", value),
                                            number
                                    );
                            return msg;
                        })
                        // allows the stream to continue after publishing messages to Kafka
                        // It accepts implementations of `ProducerMessage.Envelope` (@scaladoc[API](akka.kafka.ProducerMessage$$Envelope))
                        //  as input, which continue in the flow as implementations of `ProducerMessage.Results`
                        //  (@scaladoc[API](akka.kafka.ProducerMessage$$Results)).
                        .via(akka.kafka.javadsl.Producer.flexiFlow(producerSettings))

                        .map(result -> {
                            if (result instanceof ProducerMessage.Result) {
                                ProducerMessage.Result<String, String, Integer> res = (ProducerMessage.Result<String, String, Integer>) result;
                                ProducerRecord<String, String> record = res.message().record();
                                RecordMetadata meta = res.metadata();
                                return meta.topic() + "/" + meta.partition() + " " + res.offset() + ": " + record.value();
                            } else if (result instanceof ProducerMessage.MultiResult) {
                                ProducerMessage.MultiResult<String, String, Integer> res = (ProducerMessage.MultiResult<String, String, Integer>) result;
                                return res.getParts() .stream().map( part -> {
                                    RecordMetadata meta = part.metadata();
                                    return meta.topic() + "/" + meta.partition() + " " + part.metadata().offset() + ": " + part.record().value();
                                }).reduce((acc, s) -> acc + ", " + s);
                            } else {
                                return "passed through";
                            }
                        })
                        .runWith(Sink.foreach(System.out::println), materializer);
        // #flow

        terminateWhenDone(done);
    }

    /*
    transferMessagesFromTopicAtoTopicB

    This method is useful for cases when you need to read messages from one topic, transform or enrich them,
    and then write to another topic

    This method creates a Consumer.committableSource linked to the `topicoDelQueConsumimos` topic and then connects it
    to a Producer.commitableSink, linked to the `topicoAlQueProducimos` topic.

    The commitableSink will commit the offset back to the consumer when it has successfully published the message in the
    `topicoAlQueProducimos` topic.

    The committableSink accepts implementations ProducerMessage.Envelope (@scaladocAPI) that contain the offset to commit
    the consumption of the originating message (of type ConsumerMessage.Committable (@scaladocAPI)).

    Note that there is a risk that something fails after publishing but before committing, so commitableSink has
    "at-least-once" delivery semantics.

    So if you want to get delivery guarantees, it is better if you use the KafkaAlpakkaTransaction.java class, provided
    with this PoC. This will guarantee the "all-or-nothing", the "exactly-once" giving you the ability to spread
    transactions between multiple microservices comunicating through Kafka topics.

     */
    public void transferMessagesFromTopicAtoTopicB(String topicoDelQueConsumimos, String topicoAlQueProducimos) {
        final ConsumerSettings<String, byte[]> consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())
                        .withBootstrapServers(bootstrapServer)
                        .withGroupId(consumerGroupId)
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final ProducerSettings<String, byte[]> consumerToProducerSettings =
                ProducerSettings.create(system, new StringSerializer(), new ByteArraySerializer())
                        .withBootstrapServers(bootstrapServer);

        // #consumerToProducerFlow
        akka.kafka.javadsl.Consumer.DrainingControl<Done> control =
                akka.kafka.javadsl.Consumer.committableSource(consumerSettings, Subscriptions.topics(topicoDelQueConsumimos))
                        .map(msg -> {
                            ProducerMessage.Envelope<String, byte[], ConsumerMessage.Committable> prodMsg =
                                    new ProducerMessage.Message<>(
                                            new ProducerRecord<>(topicoAlQueProducimos, msg.record().value()),
                                            msg.committableOffset() // the passThrough
                                    );
                            return prodMsg;
                        })

                        .via(Producer.flexiFlow(consumerToProducerSettings))

                        .mapAsync(consumerToProducerSettings.parallelism(), result -> {
                            ConsumerMessage.Committable committable = result.passThrough();
                            return committable.commitJavadsl();
                        })
                        .toMat(Sink.ignore(), Keep.both())
                        .mapMaterializedValue(akka.kafka.javadsl.Consumer::createDrainingControl)
                        .run(materializer);
        // #consumerToProducerFlow
    }

    protected void terminateWhenDone(CompletionStage<Done> result) {
        result
                .exceptionally(e -> {
                    system.log().error(e, e.getMessage());
                    return Done.getInstance();
                })
                .thenAccept(d -> system.terminate());
    }

}
