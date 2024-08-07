package com.bca.kafka.KafkaOnDemandContractor.config;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.Area;
import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

    private CustomSerde serde;
    private String currentUserType;
    private Double basePrice = 10000.00;

    public KafkaStreamConfig(CustomSerde serde){
        this.serde = serde;
    }


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-construction-rate");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serde.getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, Double> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, UserCreatedEvent> stream = streamsBuilder.stream("user-position", Consumed.with(
                Serdes.String(),
                serde
        ));

        KTable<String, Long> kTable = stream.selectKey((k,v) -> {
            currentUserType = v.getType();
            if(v.getType().equalsIgnoreCase("contractor")){
                return calculateArea(v);
            }
            else{
                return "CUSTOMER";
            }
        })
        .groupByKey()
        .count(Named.as("StreamTable"));

        KStream<String, Double> priceStream = kTable.toStream()
                .peek((k,v) -> {
                    System.out.println("PEEK KEY: " + k);
                    System.out.println("PEEK VALUE: " + v);
                })
                .mapValues((k,v) -> {
                    if(currentUserType.equalsIgnoreCase("customer")){
                        return calculatePrice(v);
                    }
                    return 0.00;
                });

        priceStream.to("contractor-position-and-rate");

        return priceStream;
    }

    private Double calculatePrice(Long v){
        return basePrice - (basePrice * (v.doubleValue()/100.00));
    }

    private String calculateArea(UserCreatedEvent v) {
        if((v.getLongitude() >= 0 && v.getLongitude() <= 100) && (v.getLatitude() >= 0 && v.getLatitude() <= 100)){
            return Area.JAKARTA.toString();
        }
        else if((v.getLongitude() > 100 && v.getLongitude() <= 200) && (v.getLatitude() > 100 && v.getLatitude() <= 200)){
            return Area.BANDUNG.toString();
        }
        else{
            return Area.SURABAYA.toString();
        }
    }

}