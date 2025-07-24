package org.example.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.avro.SampleRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

/**
 * Integration test for the WordProcessor using an embedded Kafka broker. @SpringBootTest starts the
 * Spring context. @EmbeddedKafka starts an in-memory Kafka broker for the test. - topics:
 * pre-creates the specified topics. - partitions: sets the number of partitions for the topics. -
 * brokerProperties: overrides Kafka broker properties. We point the auto-created topics' offset
 * reset to "earliest" to ensure consumers read from the
 * beginning. @TestInstance(TestInstance.Lifecycle.PER_CLASS) allows @BeforeAll and @AfterAll to be
 * non-static, which is convenient for managing instance variables.
 */
@SpringBootTest
@TestPropertySource(locations = "classpath:common.properties")
@EmbeddedKafka(
        topics = {"${app.kafka.topic.private}", "${app.kafka.topic.downstream}"},
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093", "auto.create.topics.enable=false"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordProcessorTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    // Autowire the StreamsBuilderFactoryBean to get access to the underlying KafkaStreams instance.
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${app.kafka.topic.private}")
    private String inputTopic;

    @Value("${app.kafka.topic.downstream}")
    private String outputTopic;

    private KafkaTemplate<String, SampleRecord> kafkaTemplate;
    private Consumer<String, SampleRecord> consumer;

    /** Sets up the Kafka producer and consumer before any tests run. */
    @BeforeAll
    void setUp() {
        // Producer setup
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://schema-registry");
        DefaultKafkaProducerFactory<String, SampleRecord> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // Consumer setup
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("schema.registry.url", "mock://schema-registry");
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        DefaultKafkaConsumerFactory<String, SampleRecord> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(java.util.Collections.singletonList(outputTopic));
    }

    /** Cleans up the consumer after all tests are done. */
    @AfterAll
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldTransformMessageToUppercase() throws InterruptedException {
        // 1. Arrange: Wait for the Kafka Streams application to be in the RUNNING state.
        // This is a crucial step to prevent race conditions where the test sends a message
        // before the stream processor is ready to handle it. We will poll the state
        // until it's RUNNING or a timeout is reached.
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            if (System.currentTimeMillis() > timeout) {
                throw new IllegalStateException(
                        "Kafka Streams did not reach the RUNNING state within the timeout. Current state: "
                                + kafkaStreams.state());
            }
            Thread.sleep(100);
        }

        // 2. Act: Send a message to the input topic
        String key = "testKey";
        SampleRecord message = new SampleRecord(10, "hello world");
        kafkaTemplate.send(inputTopic, key, message);

        // 3. Assert: Consume the message from the output topic
        // KafkaTestUtils.getSingleRecord polls the consumer for a message for a specified duration.
        ConsumerRecord<String, SampleRecord> singleRecord =
                KafkaTestUtils.getSingleRecord(consumer, outputTopic, Duration.ofSeconds(10000L));

        // 4. Verify the consumed message is correct
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(key);
        assertThat(singleRecord.value().getNumber()).isEqualTo(-10);
        assertThat(singleRecord.value().getText()).isEqualTo("hello world - FINISH");
    }
}
