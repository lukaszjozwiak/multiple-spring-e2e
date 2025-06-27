package org.example.app;

import com.example.avro.SampleRecord;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Map;

/**
 * This class defines the Kafka Streams topology using a Spring @Configuration.
 * The @EnableKafkaStreams annotation is moved here to be co-located with the
 * streams configuration, which is a common practice.
 * <p>
 * By defining the topology within a @Bean method that accepts a StreamsBuilder
 * as a parameter, we leverage Spring's dependency injection to ensure that a
 * configured StreamsBuilder is available before our topology is defined.
 */
@Configuration
@EnableKafkaStreams
@PropertySource("classpath:common.properties")
@Slf4j
public class KafkaStreamsProcessorConfig {

    @Value("${app.kafka.topic.private}")
    private String inputTopic;

    @Value("${app.kafka.topic.downstream}")
    private String outputTopic;

    @Bean
    public KStream<String, SampleRecord> kStream(StreamsBuilder streamsBuilder, Serde<SampleRecord> avroSerde) {

        KStream<String, SampleRecord> stream = streamsBuilder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), avroSerde)
        );

        stream.mapValues(value ->
                        new SampleRecord(value.getNumber() * -1, value.getText() + " - FINISH"))
                .peek((key, value) -> log.info("Transformed value: {}", value))
                .to(outputTopic, Produced.with(Serdes.String(), avroSerde));

        return stream;
    }

    @Bean
    static <T extends SpecificRecord> Serde<T> avroSerde(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        Serde<T> serde = new SpecificAvroSerde<>();
        Map<String, Object> props = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(props, false);
        return serde;
    }
}
