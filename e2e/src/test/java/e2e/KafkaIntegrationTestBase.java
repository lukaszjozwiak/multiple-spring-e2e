package e2e;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith({SpringExtension.class})
@EmbeddedKafka(topics = {KafkaIntegrationTestBase.UPSTREAM_TOPIC, KafkaIntegrationTestBase.PRIVATE_TOPIC, KafkaIntegrationTestBase.DOWNSTREAM_TOPIC})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Slf4j
public abstract class KafkaIntegrationTestBase {

    protected static final String UPSTREAM_TOPIC = "upstream-topic";
    protected static final String PRIVATE_TOPIC = "private-topic";
    protected static final String DOWNSTREAM_TOPIC = "downstream-topic";

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    protected KafkaTemplate<String, String> kafkaTemplate;
    protected Consumer<String, String> consumer;
    private AdminClient adminClient;

    private Process appOneProcess;
    private Process appTwoProcess;
    private int appOnePort;
    private int appTwoPort;

    @TempDir
    static Path tempDirApp1;

    @TempDir
    static Path tempDirApp2;

    private static final TestRestTemplate testRestTemplate = new TestRestTemplate();

    @BeforeAll
    void beforeAll() throws Exception {
        initConsumer("test-consumer");
        initProducer();
        initAdminClient();
        initAndStartApplications(); // Renamed for clarity
    }

    private void initAndStartApplications() throws Exception {
        log.info("Starting application 1...");
        startApp("../app1/target/app1-1.0-SNAPSHOT.jar", tempDirApp1, ((process, port) -> {
            appOneProcess = process;
            appOnePort = port;
        }));

        log.info("Starting application 2...");
        startApp("../app2/target/app2-1.0-SNAPSHOT.jar", tempDirApp2, ((process, port) -> {
            appTwoProcess = process;
            appTwoPort = port;
        }));

        waitForApp(appOnePort, "App1"); // Wait for App1 to be healthy
        waitForApp(appTwoPort, "App2"); // Wait for App2 to be healthy
    }

    private void initAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBroker.getBrokersAsString());
        this.adminClient = AdminClient.create(props);
    }

    private void initProducer() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    protected void initConsumer(String consumerGroup) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(consumerGroup, "false", kafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
    }

    private void startApp(String appPath, Path tempDir, BiConsumer<Process, Integer> initializer) throws Exception {
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
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForApp(int port, String appName) {
        log.info("Waiting for {} on port {} to be healthy...", appName, port);

        HttpRequest healthCheckRequest = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://localhost:%d/actuator/health", port)))
                .timeout(Duration.ofSeconds(1))
                .GET()
                .build();

        await()
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> {
                    try (HttpClient httpClient = HttpClient.newHttpClient()) {
                        HttpResponse<String> response = httpClient.send(healthCheckRequest, HttpResponse.BodyHandlers.ofString());
                        assertThat(response.statusCode()).isEqualTo(200);
                        assertThat(response.body()).isNotNull();
                        assertThat(response.body()).contains("\"status\":\"UP\"");
                    }
                });

        log.info("{} on port {} is up!", appName, port);
    }

    @AfterAll
    void stopAll() {
        log.info("Stopping all integration test components...");
        stopConsumer();
        stopProcess(appOneProcess, "App1");
        stopProcess(appTwoProcess, "App2");
        stopAdminClient();
        stopKafkaBroker();
        log.info("All components stopped.");
    }

    private void stopConsumer() {
        if (consumer != null) {
            consumer.close();
            log.info("Kafka Consumer stopped.");
        }
    }

    private void stopKafkaBroker() {
        if (kafkaBroker != null) {
            kafkaBroker.destroy();
            log.info("Embedded Kafka stopped.");
        }
    }

    private void stopAdminClient() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }

    private static void stopProcess(Process process, String appName) {
        if (process != null) {
            log.info("Stopping process for {}...", appName);
            process.destroy();
            try {
                if (process.waitFor(10, TimeUnit.SECONDS)) {
                    log.info("Process for {} stopped gracefully.", appName);
                } else {
                    log.warn("Process for {} did not stop gracefully after 10 seconds. Forcing shutdown.", appName);
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for process {} to stop. Forcing shutdown.", appName, e);
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    @AfterEach
    void clearKafkaTopics() throws Exception {
        Set<String> topics = adminClient.listTopics().names().get();
        Set<String> topicsToClear = topics.stream()
                .filter(topicName -> !topicName.startsWith("__"))
                .collect(Collectors.toSet());

        if (topicsToClear.isEmpty()) {
            return;
        }

        log.info("Clearing Kafka topics after test: {}", topicsToClear);

        Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topicsToClear).allTopicNames().get();

        Map<TopicPartition, RecordsToDelete> recordsToDelete = topicDescriptions.values().stream()
                .flatMap(desc -> desc.partitions().stream()
                        .map(p -> new TopicPartition(desc.name(), p.partition())))
                .collect(Collectors.toMap(tp -> tp, tp -> RecordsToDelete.beforeOffset(-1L)));

        if (!recordsToDelete.isEmpty()) {
            adminClient.deleteRecords(recordsToDelete).all().get();
            log.info("Topics cleared successfully.");
        }
    }
}