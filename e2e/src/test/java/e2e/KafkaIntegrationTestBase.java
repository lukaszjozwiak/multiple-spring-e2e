package e2e;

import io.restassured.RestAssured;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public abstract class KafkaIntegrationTestBase {

    protected static final String UPSTREAM_TOPIC = "upstream-topic";
    protected static final String PRIVATE_TOPIC = "private-topic";
    protected static final String DOWNSTREAM_TOPIC = "downstream-topic";

    private static EmbeddedKafkaBroker kafkaBroker;
    protected static KafkaTemplate<String, String> kafkaTemplate;
    protected static Consumer<String, String> consumer;
    private static AdminClient adminClient;

    private static Process appOneProcess;
    private static Process appTwoProcess;
    private static int appOnePort;
    private static int appTwoPort;

    @TempDir
    static Path tempDirApp1;

    @TempDir
    static Path tempDirApp2;

    @BeforeAll
    static void beforeAll() throws Exception {
        initKafkaBroker(UPSTREAM_TOPIC, PRIVATE_TOPIC, DOWNSTREAM_TOPIC);
        initConsumer("test-consumer");
        initProducer();
        initAdminClient();
        initApplications();
    }

    @BeforeAll
    static void initApplications() throws Exception {
        startApp("../app1/target/app1-1.0-SNAPSHOT.jar", tempDirApp1, ((process, port) -> {
            appOneProcess = process;
            appOnePort = port;
        }));

        startApp("../app2/target/app2-1.0-SNAPSHOT.jar", tempDirApp2, ((process, port) -> {
            appTwoProcess = process;
            appTwoPort = port;
        }));
    }

    private static void initKafkaBroker(String... topics) {
        kafkaBroker = new EmbeddedKafkaZKBroker(1, true, 2, topics).kafkaPorts(0);
        kafkaBroker.afterPropertiesSet();
    }

    private static void initAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        adminClient = AdminClient.create(props);

    }

    private static void initProducer() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    protected static void initConsumer(String consumerGroup) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(consumerGroup, "false", kafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
    }

    private static void startApp(String appPath, Path tempDir, BiConsumer<Process, Integer> initializer) throws Exception {
        Integer port = findFreePort();
        Path path = Paths.get(appPath);
        String jarName = path.getFileName().toString();
        File jarPath = new File(appPath);

        if (!jarPath.exists()) {
            throw new IllegalStateException("JAR file not found for " + appPath + " at " + jarPath.getAbsolutePath());
        }

        List<String> processCommands = List.of(
                "java",
                "-jar",
                path.toAbsolutePath().toString(),
                "--server.port=" + port,
                "--spring.kafka.bootstrap-servers=" + kafkaBroker.getBrokersAsString()
        );

        log.info("Starting app {} with commands: {}", appPath, String.join(" ", processCommands));

        ProcessBuilder builder = new ProcessBuilder(processCommands);

        builder.redirectError(new File(tempDir.toFile(), jarName + "-error.log"));
        builder.redirectOutput(new File(tempDir.toFile(), jarName + "-output.log"));

        Process process = builder.start();
        initializer.accept(process, port);

        log.info("{} on port {} is up!", jarName, port);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForApp(int port) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> {
                    RestAssured.given()
                            .port(port)
                            .when()
                            .get("/actuator/health")
                            .then()
                            .statusCode(200)
                            .body("status", CoreMatchers.equalTo("UP"));
                });
    }

    @AfterAll
    static void stopApplications() throws Exception {
        stopConsumer();
        stopProcess(appOneProcess);
        stopProcess(appTwoProcess);
        stopKafkaBroker();
    }

    private static void stopConsumer() {
        if (consumer != null) {
            consumer.close();
            log.info("Kafka Consumer stopped.");
        }
    }

    private static void stopKafkaBroker() {
        if (kafkaBroker != null) {
            kafkaBroker.destroy();
            log.info("Embedded Kafka stopped.");
        }
    }

    private static void stopProcess(Process process) throws InterruptedException {
        if (process != null) {
            process.destroy();
            process.waitFor(5, TimeUnit.SECONDS);
            if (process.isAlive()) process.destroyForcibly();
        }
    }

    @AfterEach
    void clearKafkaTopics() throws Exception {
        // Get all topics
        Set<String> topics = adminClient.listTopics().names().get();

        // Filter out internal Kafka topics (e.g., __consumer_offsets, __transaction_state)
        Set<String> topicsToClear = topics.stream()
                .filter(topicName -> !topicName.startsWith("__"))
                .collect(Collectors.toSet());

        if (topicsToClear.isEmpty()) {
            return; // Nothing to do
        }

        log.info("Clearing Kafka topics after test: {}", topicsToClear);

        // Describe topics to get their partitions
        Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topicsToClear).allTopicNames().get();

        // Create a map of TopicPartition to RecordsToDelete.
        // RecordsToDelete.beforeOffset(-1) signifies deleting all records.
        Map<TopicPartition, RecordsToDelete> recordsToDelete = topicDescriptions.values().stream()
                .flatMap(desc -> desc.partitions().stream()
                        .map(p -> new TopicPartition(desc.name(), p.partition())))
                .collect(Collectors.toMap(tp -> tp, tp -> RecordsToDelete.beforeOffset(-1L)));

        // Execute the deletion
        adminClient.deleteRecords(recordsToDelete).all().get();
        log.info("Topics cleared successfully.");
    }
}
