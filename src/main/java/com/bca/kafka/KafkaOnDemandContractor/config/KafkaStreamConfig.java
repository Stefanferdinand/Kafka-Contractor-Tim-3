package com.bca.kafka.KafkaOnDemandContractor.config;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-construction-rate");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        return new KafkaStreamsConfiguration(props);
    }
    @Bean
    public KStream<String, UserCreatedEvent> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, UserCreatedEvent> sourceStream = streamsBuilder.stream("user-position",
                Consumed.with(Serdes.String(), new JsonSerde<>(UserCreatedEvent.class)));

        KStream<String, String> rateStream = sourceStream.mapValues(value -> {
            // Perform rate calculation based on user position
            return calculateRate(value);
        });

        rateStream.to("constructors-position-and-rate", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }

    private String calculateRate(UserCreatedEvent userPosition) {
        // Dummy rate calculation logic
        return "rate-calculation";
    }
}