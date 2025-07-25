package e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.avro.SampleRecord;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@Slf4j
class FullSystemIT extends KafkaIntegrationTestBase {

	@Test
	void testFullFlowWithAvroSchema() {
		String key = "testKey";
		SampleRecord message = new SampleRecord(10, "hello world");
		kafkaTemplate.send(UPSTREAM_TOPIC, key, message);

		consumer.subscribe(java.util.Collections.singletonList(DOWNSTREAM_TOPIC));
		ConsumerRecord<String, SampleRecord> singleRecord = KafkaTestUtils.getSingleRecord(consumer, DOWNSTREAM_TOPIC,
				Duration.ofSeconds(10000L));

		assertThat(singleRecord).isNotNull();
		assertThat(singleRecord.key()).isEqualTo(key);
		assertThat(singleRecord.value().getNumber()).isEqualTo(-20);
		assertThat(singleRecord.value().getText()).isEqualTo("HELLO WORLD - FINISH");
	}

}
