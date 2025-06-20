package e2e;

import io.restassured.RestAssured;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class FullSystemIT {

    private static final String UPSTREAM_TOPIC = "upstream-topic";
    private static final String PRIVATE_TOPIC = "private-topic";
    private static final String DOWNSTREAM_TOPIC = "downstream-topic";

    private static EmbeddedKafkaBroker embeddedKafka;

    private static Process appOneProcess;
    private static Process appTwoProcess;
    private static int appOnePort;
    private static int appTwoPort;

    @TempDir
    static Path tempDir;

    private static KafkaTemplate<String, String> kafkaTemplate;
    private static Consumer<String, String> consumer;

    @BeforeAll
    static void startApplications() throws IOException, InterruptedException {

        String bootstrapServers = initKafka();
        // 1. Find free ports
        appOnePort = findFreePort();
        appTwoPort = findFreePort();

        // 2. Start the processes
        appOneProcess = startApp("../app1/target/app1-1.0-SNAPSHOT.jar", bootstrapServers, Set.of(), appOnePort);
        appTwoProcess = startApp("../app2/target/app2-1.0-SNAPSHOT.jar", bootstrapServers, Set.of(), appTwoPort);

        // 3. Wait for applications to be healthy
        waitForApp("App 1", appOnePort);
        waitForApp("App 2", appTwoPort);

        log.info("Both applications started successfully.");
    }

    static String initKafka() {
        embeddedKafka = new EmbeddedKafkaZKBroker(1, true, 2, UPSTREAM_TOPIC, PRIVATE_TOPIC, DOWNSTREAM_TOPIC)
                .kafkaPorts(0);
        embeddedKafka.afterPropertiesSet();


        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // Consumer setup
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(java.util.Collections.singletonList(DOWNSTREAM_TOPIC));

        return embeddedKafka.getBrokersAsString();
    }


    @AfterAll
    static void stopApplications() throws InterruptedException {
        // 5. Terminate processes
        if (appOneProcess != null) {
            appOneProcess.destroy();
            appOneProcess.waitFor(5, TimeUnit.SECONDS);
            if (appOneProcess.isAlive()) appOneProcess.destroyForcibly();
        }
        if (appTwoProcess != null) {
            appTwoProcess.destroy();
            appTwoProcess.waitFor(5, TimeUnit.SECONDS);
            if (appTwoProcess.isAlive()) appTwoProcess.destroyForcibly();
        }
        log.info("Both applications stopped.");

        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testApp1() throws Exception {
        // 2. Act: Send a message to the input topic
        String key = "testKey";
        String message = "hello world";
        Thread.sleep(Duration.ofSeconds(60).toMillis());
        kafkaTemplate.send(UPSTREAM_TOPIC, key, message);

        // 3. Assert: Consume the message from the output topic
        // KafkaTestUtils.getSingleRecord polls the consumer for a message for a specified duration.
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, DOWNSTREAM_TOPIC, Duration.ofSeconds(10000L));


        // 4. Verify the consumed message is correct
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(key);
        assertThat(singleRecord.value()).isEqualTo("HELLO WORLD - FINISH");
    }


    private static Process startApp(String appPath, String bootstrapServers, Set<String> profiles, int port) throws IOException {
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
                "--spring.profiles.active=" + String.join(",", profiles),
                "--spring.kafka.bootstrap-servers=" + bootstrapServers
        );

        log.info("Starting app {} with commands: {}", appPath, String.join(" ", processCommands));

        ProcessBuilder builder = new ProcessBuilder(processCommands);

        // Redirect process output to log files for easier debugging
        builder.redirectError(new File(tempDir.toFile(), jarName + "-error.log"));
        builder.redirectOutput(new File(tempDir.toFile(), jarName + "-output.log"));

        log.info("Starting {}", appPath);

        return builder.start();
    }

    @AfterAll
    static void stopAll() throws InterruptedException {
        // Stop apps first
        stopProcess(appOneProcess);
        stopProcess(appTwoProcess);

        // 4. Stop the embedded Kafka broker
        if (embeddedKafka != null) {
            embeddedKafka.destroy();
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

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForApp(String appName, int port) throws InterruptedException {
        long timeout = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();

        while (System.currentTimeMillis() < timeout) {
            try {
                // Use REST Assured to check the health endpoint
                RestAssured.given()
                        .port(port)
                        .when()
                        .get("/actuator/health")
                        .then()
                        .statusCode(200)
                        .body("status", CoreMatchers.equalTo("UP")); // More robust: check status is UP

                log.info("{} on port {} is up!", appName, port);
                return; // Success, exit the loop
            } catch (Exception e) {
                // Catches ConnectionRefusedException and other initial connection errors from REST Assured
                // App not ready yet, wait and retry
                Thread.sleep(2500);
            }
        }
        throw new RuntimeException(appName + " did not start in time or did not report a healthy status.");
    }
}