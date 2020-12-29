import net.christophschubert.cp.testcontainers.CPTestContainer;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.KafkaConnectContainer;
import net.christophschubert.cp.testcontainers.util.LogWaiter;
import net.christophschubert.kafka.connect.client.ConnectClient;
import net.christophschubert.kafka.connect.client.ConnectorConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CastTest {
    @Test
    public void setupReplicator() throws InterruptedException, IOException, ExecutionException {
        final Network network = Network.newNetwork();
        final var testContainerFactory = new CPTestContainerFactory(network);


        final KafkaContainer sourceKafka = testContainerFactory.createKafka();
        final KafkaContainer destinationKafka = testContainerFactory.createKafka();
        sourceKafka.start();
        destinationKafka.start();

        final LogWaiter waiter = new LogWaiter("INFO Successfully started up Replicator source task");

        final KafkaConnectContainer replicatorContainer = testContainerFactory.createReplicator(destinationKafka)
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/extras")
                .withFileSystemBind("./build/libs", "/extras", BindMode.READ_ONLY);
        replicatorContainer.withLogConsumer(outputFrame -> waiter.accept(outputFrame.getUtf8String()));
        replicatorContainer.start();

        //pre-create topics:
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers()));
        adminClient.createTopics(List.of(new NewTopic("data.topic", Optional.empty(), Optional.empty()))).all().get();

        final var replicatorConfig = ConnectorConfig.source("replicator-data", "io.confluent.connect.replicator.ReplicatorSourceConnector")
                .withTopicRegex("data\\..*")
                .with("topic.rename.format", "${topic}.replica")
                .withKeyConverter("io.confluent.connect.replicator.util.ByteArrayConverter")
                .withValueConverter("io.confluent.connect.replicator.util.ByteArrayConverter")
                .withTransforms("caster")
                .with("transforms.caster.type", "net.christophschubert.kafka.connect.smt.CastBinary")
                .with("transforms.caster.cast.to", "String")
                .with("src.kafka.bootstrap.servers", CPTestContainer.getInternalBootstrap(sourceKafka));

        final ConnectClient connectClient = new ConnectClient(replicatorContainer.getBaseUrl());
        connectClient.startConnector(replicatorConfig);

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);

        final Producer<String, ByteBuffer> producer = new KafkaProducer<>(producerProperties);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of("data.topic.replica"));

        final byte[] testValue = "some-value".getBytes();
        producer.send(new ProducerRecord<>("data.topic", "user", ByteBuffer.wrap(testValue)));
        producer.flush();

        var msgCount = 0;

        while (!waiter.found) {
            for (int i = 0; i < 2; i++) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                    Assert.assertEquals("some-value", record.value());
                    ++msgCount;
                }
            }

        }
        Assert.assertEquals(1, msgCount);
    }
}
