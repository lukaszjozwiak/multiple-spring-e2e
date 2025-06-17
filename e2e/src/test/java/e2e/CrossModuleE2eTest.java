package e2e;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
class CrossModuleE2eTest {

    private static EmbeddedKafkaBroker broker;
    private static ConfigurableApplicationContext listener;
    private static ConfigurableApplicationContext producer;

    private static final String INPUT_TOPIC = "upstream-topic";
    private static final String PRIVATE_TOPIC = "private-topic";
    private static final String OUTPUT_TOPIC = "downstream-topic";

    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<String, String> consumer;

    @BeforeAll
    static void setup() throws Exception {
        initEmbeddedKafka();
        initApplicationsUnderTest();
    }

    static void initEmbeddedKafka() throws MalformedURLException {
        broker = new EmbeddedKafkaKraftBroker(1, 1,
                INPUT_TOPIC,
                PRIVATE_TOPIC,
                OUTPUT_TOPIC);
        broker.afterPropertiesSet();
    }

    static void initApplicationsUnderTest() throws Exception {

        CountDownLatch app1Listener = new CountDownLatch(1);
        CountDownLatch app2Listener = new CountDownLatch(1);


        URLClassLoader listenerCL = loaderFor("../app1");
        Class<?> app1Main = listenerCL.loadClass("org.example.app.App1Application");
        DefaultResourceLoader listenerRL = new DefaultResourceLoader(listenerCL);

        Map<String, Object> propsApp1 = Map.of(
                "spring.kafka.bootstrap-servers", broker.getBrokersAsString(),
                "app.kafka.topic.input", INPUT_TOPIC,
                "app.kafka.topic.output", PRIVATE_TOPIC,
                "spring.kafka.streams.application-id", "listener-e2e"
        );

        listener = new SpringApplicationBuilder(listenerRL, app1Main)
                .resourceLoader(listenerRL)
                .properties(propsApp1)
                .listeners((ApplicationReadyEvent e) -> app1Listener.countDown())
                .run();

        URLClassLoader producerCL = loaderFor("../app2");
        Class<?> app2Main = producerCL.loadClass("org.example.app.App2Application");
        DefaultResourceLoader producerRL = new DefaultResourceLoader(producerCL);

        Map<String, Object> propsApp2 = Map.of(
                "spring.kafka.bootstrap-servers", broker.getBrokersAsString(),
                "app.kafka.topic.input", PRIVATE_TOPIC,
                "app.kafka.topic.output", OUTPUT_TOPIC,
                "spring.kafka.streams.application-id", "producer-e2e"
        );

        producer = new SpringApplicationBuilder(producerRL, app2Main)
                .resourceLoader(producerRL)
                .properties(propsApp2)
                .listeners((ApplicationReadyEvent e) -> app2Listener.countDown())
                .run();

        app1Listener.await(60, TimeUnit.SECONDS);
        app2Listener.await(60, TimeUnit.SECONDS);
    }

    static URLClassLoader loaderFor(String moduleDir) throws MalformedURLException {
        return new URLClassLoader(
                new URL[]{Paths.get(moduleDir, "target", "classes").toUri().toURL()},
                null
        );
    }

    @BeforeEach
    void initKafkaClients() {
        // Producer setup
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // Consumer setup
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", broker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(java.util.Collections.singletonList(OUTPUT_TOPIC));
    }


    @Test
    void shouldRunBothModules() {
        // given
        String key = "testKey";
        String message = "hello world";

        // when
        kafkaTemplate.send(INPUT_TOPIC, key, message);
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, OUTPUT_TOPIC, Duration.ofSeconds(30L));

        // then
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(key);
        assertThat(singleRecord.value()).isEqualTo("HELLO WORLD - FINISHED");
    }


    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @AfterAll
    static void tearDownAll() {
//        producer.close();
//        listener.close();
//        broker.destroy();
    }
}
