package e2e;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
class SchemaRegistryServer {

    private final EmbeddedKafkaBroker kafkaBroker;
    private Server server;

    @PostConstruct
    void startSchemaRegistry() throws Exception {
        int port = findFreePort();
        Properties properties = new Properties();
        properties.put(SchemaRegistryConfig.LISTENERS_CONFIG, "http://0.0.0.0:" + port);
        properties.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        properties.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas");

        SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(new SchemaRegistryConfig(properties));
        this.server = app.createServer();
        this.server.start();
        log.info("Embedded Schema Registry started at: {}", this.server.getURI());
    }

    @PreDestroy
    @SneakyThrows
    private void stopSchemaRegistryServer() {
        if (this.server != null) {
            this.server.stop();
            log.info("Embedded Schema Registry stopped.");
        }
    }

    String getSchemaRegistryUrl() {
        return this.server.getURI().toString();
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
