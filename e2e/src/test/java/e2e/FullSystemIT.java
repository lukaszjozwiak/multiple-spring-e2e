package e2e;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class FullSystemIT extends KafkaIntegrationTestBase {

    @Test
    void testApp1() throws Exception {
        String key = "testKey";
        String message = "hello world";
        kafkaTemplate.send(UPSTREAM_TOPIC, key, message);

        consumer.subscribe(java.util.Collections.singletonList(DOWNSTREAM_TOPIC));
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, DOWNSTREAM_TOPIC, Duration.ofSeconds(10000L));

        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(key);
        assertThat(singleRecord.value()).isEqualTo("HELLO WORLD - FINISH");
    }

    @Test
    void testApp2() throws Exception {
        String key = "testKey";
        String message = "ala ma kota";
        kafkaTemplate.send(UPSTREAM_TOPIC, key, message);

        consumer.subscribe(java.util.Collections.singletonList(DOWNSTREAM_TOPIC));
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, DOWNSTREAM_TOPIC, Duration.ofSeconds(10000L));

        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(key);
        assertThat(singleRecord.value()).isEqualTo("ALA MA KOTA - FINISH");
    }
}