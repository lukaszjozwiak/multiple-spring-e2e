package e2e;

import com.example.avro.SampleRecord;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@Slf4j
@SpringBootConfiguration
@EnableAutoConfiguration
class ItConfig {

	@Bean
	public MockSchemaRegistryClient mockSchemaRegistryClient() {
		return new MockSchemaRegistryClient();
	}

	@Bean
	AdminClient adminClient(EmbeddedKafkaBroker kafkaBroker) {
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
		return AdminClient.create(props);
	}

	@Bean
	@Lazy
	Consumer<String, SampleRecord> consumer(@Value("${local.server.port}") int localServerPort,
			EmbeddedKafkaBroker kafkaBroker) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "false", kafkaBroker);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		consumerProps.put("schema.registry.url", "http://localhost:" + localServerPort);
		consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
		DefaultKafkaConsumerFactory<String, SampleRecord> consumerFactory = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		return consumerFactory.createConsumer();
	}

	@Bean
	@Lazy
	KafkaTemplate<String, SampleRecord> kafkaTemplate(@Value("${local.server.port}") int localServerPort,
			EmbeddedKafkaBroker kafkaBroker) {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBroker);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		producerProps.put("schema.registry.url", "http://localhost:" + localServerPort);
		DefaultKafkaProducerFactory<String, SampleRecord> producerFactory = new DefaultKafkaProducerFactory<>(
				producerProps);
		return new KafkaTemplate<>(producerFactory);
	}

}
