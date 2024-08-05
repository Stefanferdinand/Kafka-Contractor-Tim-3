package com.bca.kafka.KafkaOnDemandContractor;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KafkaOnDemandContractorStream{

    public static void main(String[] args) {

        // setup stream properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-construction-rate");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        // Stream builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Kafka stream read topic
        KStream<String, UserCreatedEvent> sourceStream = streamsBuilder.stream("user-position",
                Consumed.with(Serdes.String(), new JsonSerde<>(UserCreatedEvent.class)));

        System.out.println("STREAM START");

        KStream<String, String> rateStream = sourceStream.mapValues(value -> {
            // Perform rate calculation based on user position
            System.out.println("KSTREAM VALUE: " + value.getUsername());
            return value.getUsername();
//            return calculateRate(value);
        });

        rateStream.to("constructors-position-and-rate", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        // add graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

}
