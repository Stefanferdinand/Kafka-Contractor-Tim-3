package com.bca.kafka.KafkaOnDemandContractor.config;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

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
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(UserCreatedEvent.class)).getClass().getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, UserCreatedEvent> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, UserCreatedEvent> stream = streamsBuilder.stream("user-position", Consumed.with(
                Serdes.String(),
                Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(UserCreatedEvent.class))
        ));
        stream.foreach((key, value) -> {
            System.out.println("Key: " + key + ", Value: " + value);
            System.out.println("username: " + value.getUsername());
            System.out.println("type: " + value.getType());
            System.out.println("longitude: " + value.getLongitude());
            System.out.println("latitude: " + value.getLatitude());
        });
        stream.to("contractor-position-and-rate");
        return stream;
    }

}