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
import java.util.function.Consumer;

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
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, Double> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, UserCreatedEvent> stream = streamsBuilder.stream("user-position", Consumed.with(
                Serdes.String(),
                serde
        ));

        System.out.println("Start Calculating");
        KTable<String,Long>kCountContractorTable= stream.filter(
                        (k,v)->v.getType().equalsIgnoreCase("contractor"))
                .selectKey((k,v) -> {
                    currentUserType = v.getType();
                    return calculateArea(v);
                }).groupByKey().count();

        KTable<String,Long>kCountCustomerTable = stream.filter(
                (k,v)->v.getType().equalsIgnoreCase("customer"))
                .selectKey((k,v) -> {
            currentUserType = v.getType();
            return calculateArea(v);
        }).groupByKey().count();
        KTable<String, Double> joinedTable = kCountContractorTable.join(
                kCountCustomerTable,
                (contractorCount, customerCount) -> calculatePrice(customerCount, contractorCount));

        joinedTable.toStream().to("contractor-position-and-rate");
        joinedTable.toStream().peek((k,v) -> {
            System.out.println("JOIN KEY: " + k);
            System.out.println("JOIN VALUE: " + v);
        }).toTable();

        System.out.println("KTABLE COUNT CONTRACTOR: " + kCountContractorTable);
        kCountCustomerTable.toStream().foreach((k,v) -> {
            System.out.println("CUSTOMER AREA: " + k);
            System.out.println("CUSTOMER COUNT: " + v);
        });
        kCountContractorTable.toStream().foreach((k,v) -> {
            System.out.println("CONTRACTOR AREA: " + k);
            System.out.println("CONTRACTOR COUNT: " + v);
        });

        System.out.println("Finish Calculating");


        return null;
    }

    private Double calculatePrice(Long customer, Long contractor){
//        10000 - (10000 * (2/100))
        Double sensitivity = 0.02;
        System.out.println("CUSTOMER: " + customer.doubleValue());
        System.out.println("CONTRACTOR: " + contractor.doubleValue());
        Double total = basePrice *(1.0+ sensitivity.doubleValue() * ((customer.doubleValue()/contractor.doubleValue())-1.0));
        System.out.println("Real Contractor Rate " + total);
        if(total<basePrice)
        {
            return basePrice;
        }
        else{
            return total;
        }

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