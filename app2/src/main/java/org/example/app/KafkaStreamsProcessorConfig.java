package org.example.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

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
@Slf4j
public class KafkaStreamsProcessorConfig {

    @Value("${app.kafka.topic.input}")
    private String inputTopic;

    @Value("${app.kafka.topic.output}")
    private String outputTopic;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        // Define the input stream from the specified topic.
        KStream<String, String> stream = streamsBuilder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Process the stream: for each message, convert its value to uppercase.
        stream.mapValues(value -> value + " - FINISH")
                // Optional: Log the transformed value to the console for debugging.
                .peek((key, value) -> log.info("Transformed value: {}", value))
                // Write the transformed stream to the output topic.
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // The KStream is returned, but the primary purpose of this bean is to build the topology.
        return stream;
    }
}
