package e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.avro.SampleRecord;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = ItConfig.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = MockSchemaRegistryController.class)
@TestPropertySource(locations = "classpath:common.properties")
@EmbeddedKafka(
		topics = { "${app.kafka.topic.upstream}", "${app.kafka.topic.private}", "${app.kafka.topic.downstream}" })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Slf4j
public abstract class KafkaIntegrationTestBase {

	protected static final String UPSTREAM_TOPIC = "upstream-topic";

	protected static final String DOWNSTREAM_TOPIC = "downstream-topic";

	@LocalServerPort
	private int localServerPort;

	@Value("${project.version}")
	private String projectVersion;

	@Autowired
	private EmbeddedKafkaBroker kafkaBroker;

	@Autowired
	protected KafkaTemplate<String, SampleRecord> kafkaTemplate;

	@Autowired
	protected Consumer<String, SampleRecord> consumer;

	@Autowired
	private AdminClient adminClient;

	private Process appOneProcess;

	private Process appTwoProcess;

	private int appOnePort;

	private int appTwoPort;

	@TempDir
	static Path tempDirApp;

	@BeforeAll
	void beforeAll() throws Exception {
		initAndStartApplications();
	}

	private void initAndStartApplications() throws Exception {
		CountDownLatch readyLatch = new CountDownLatch(2);

		try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
			executor.submit(() -> {
				try {
					log.info("Starting application 1...");
					startApp("../app1/target/app1-%s.jar".formatted(projectVersion), ((process, port) -> {
						appOneProcess = process;
						appOnePort = port;
					}));
				}
				finally {
					readyLatch.countDown();
				}
			});

			executor.submit(() -> {
				try {
					log.info("Starting application 2...");
					startApp("../app2/target/app2-%s.jar".formatted(projectVersion), ((process, port) -> {
						appTwoProcess = process;
						appTwoPort = port;
					}));
				}
				finally {
					readyLatch.countDown();
				}
			});
		}

		boolean appsAreReady = readyLatch.await(2, TimeUnit.MINUTES);

		if (!appsAreReady) {
			throw new IllegalStateException("Timeout exceeded while waiting for applications to start.");
		}

		log.info("Both applications are healthy. Proceeding with tests.");
	}

	@SneakyThrows
	private void startApp(String appPath, BiConsumer<Process, Integer> initializer) {

		Integer port = findFreePort();
		Path path = Paths.get(appPath);

		String jarName = path.getFileName().toString();
		File jarPath = new File(appPath);

		if (!jarPath.exists()) {
			throw new IllegalStateException("JAR file not found for " + appPath + " at " + jarPath.getAbsolutePath());
		}

		List<String> processCommands = List.of("java", "-jar", path.toAbsolutePath().toString(),
				"--server.port=" + port, "--spring.kafka.bootstrap-servers=" + kafkaBroker.getBrokersAsString(),
				"--spring.kafka.properties.schema.registry.url=" + "http://localhost:" + localServerPort);

		log.info("Starting app {} with commands: {}", appPath, String.join(" ", processCommands));
		ProcessBuilder builder = new ProcessBuilder(processCommands);
		builder.redirectError(new File(tempDirApp.toFile(), jarName + "-error.log"));
		builder.redirectOutput(new File(tempDirApp.toFile(), jarName + "-output.log"));

		Process process = builder.start();
		initializer.accept(process, port);
		waitForApp(appOnePort, jarName);
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

		await().atMost(Duration.ofSeconds(90))
			.pollInterval(Duration.ofSeconds(2))
			.ignoreExceptions()
			.untilAsserted(() -> {
				try (HttpClient httpClient = HttpClient.newHttpClient()) {
					HttpResponse<String> response = httpClient.send(healthCheckRequest,
							HttpResponse.BodyHandlers.ofString());
					assertThat(response.statusCode()).isEqualTo(200);
					assertThat(response.body()).isNotNull();
					assertThat(response.body()).contains("\"status\":\"UP\"");
				}
			});

		log.info("{} on port {} is up!", appName, port);
	}

	@AfterAll
	@SneakyThrows
	void stopAll() {
		log.info("Stopping all integration test components...");
		stopConsumer();
		CountDownLatch readyLatch = new CountDownLatch(2);

		try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
			executor.submit(() -> {
				try {
					stopProcess(appOneProcess, "App1");
				}
				finally {
					readyLatch.countDown();
				}
			});
			executor.submit(() -> {
				try {
					stopProcess(appTwoProcess, "App2");
				}
				finally {
					readyLatch.countDown();
				}
			});
		}

		boolean appsAreReady = readyLatch.await(2, TimeUnit.MINUTES);

		if (!appsAreReady) {
			throw new IllegalStateException("Timeout exceeded while waiting for applications to stop.");
		}

		log.info("Both applications stopped");

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
				}
				else {
					log.warn("Process for {} did not stop gracefully after 10 seconds. Forcing shutdown.", appName);
					process.destroyForcibly();
				}
			}
			catch (InterruptedException e) {
				log.error("Interrupted while waiting for process {} to stop. Forcing shutdown.", appName, e);
				process.destroyForcibly();
				Thread.currentThread().interrupt();
			}
		}
	}

	@AfterEach
	void clearKafkaTopics() throws Exception {

		// 1. all user-visible topics (skip internal "__")
		Set<String> topics = adminClient.listTopics()
			.names()
			.get()
			.stream()
			.filter(t -> !t.startsWith("__"))
			.collect(Collectors.toSet());
		if (topics.isEmpty())
			return;

		// 2. fetch metadata + config for every topic
		Map<String, TopicDescription> desc = adminClient.describeTopics(topics).allTopicNames().get();

		Set<ConfigResource> resources = desc.keySet()
			.stream()
			.map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
			.collect(Collectors.toSet());
		Map<ConfigResource, Config> cfg = adminClient.describeConfigs(resources).all().get();

		// 3. only topics whose cleanup.policy == delete (policy allows truncate)
		Set<String> deletableTopics = cfg.entrySet().stream().filter(e -> {
			String v = e.getValue().get("cleanup.policy").value(); // null → defaults to
																	// delete
			return v == null || "delete".equals(v);
		}).map(e -> e.getKey().name()).collect(Collectors.toSet());

		if (deletableTopics.isEmpty())
			return;

		// 4. build RecordsToDelete request
		Map<TopicPartition, RecordsToDelete> recordsToDelete = desc.values()
			.stream()
			.filter(d -> deletableTopics.contains(d.name()))
			.flatMap(d -> d.partitions().stream().map(p -> new TopicPartition(d.name(), p.partition())))
			.collect(Collectors.toMap(tp -> tp, tp -> RecordsToDelete.beforeOffset(-1L)));

		adminClient.deleteRecords(recordsToDelete).all().get();
		log.info("Cleared Kafka topics: {}", deletableTopics);
	}

}
