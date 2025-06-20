package e2e;

import io.restassured.RestAssured;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
class FullSystemIT {

    private static EmbeddedKafkaBroker embeddedKafka;
    private static Process appOneProcess;
    private static Process appTwoProcess;
    private static int appOnePort;
    private static int appTwoPort;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startApplications() throws IOException, InterruptedException {

        // 2. Start the embedded Kafka broker FIRST

        embeddedKafka = new EmbeddedKafkaKraftBroker(1, 2, "upstream-topic", "private-topic", "downstream-topic")
                .kafkaPorts(0);
        embeddedKafka.afterPropertiesSet();

        String bootstrapServers = embeddedKafka.getBrokersAsString();
        log.info("Bootstrap servers: {}", bootstrapServers);

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
    }

    @Test
    void testApp1() throws Exception {
        log.info("App 1");
    }

    @Test
    void systemShouldBeHealthy() {
        // This test now also uses REST Assured for consistency
        RestAssured.given()
                .port(appOnePort)
                .when()
                .get("/actuator/health")
                .then()
                .statusCode(200)
                .body("status", CoreMatchers.equalTo("UP"))
                .body(CoreMatchers.containsString("\"kafka\"")); // Verify kafka component in health check
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