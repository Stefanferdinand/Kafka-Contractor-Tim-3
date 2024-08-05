package com.bca.kafka.KafkaOnDemandContractor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class TopicConfig {


    /*
    use this command to delete all the topics in your local server

    for topic in $(kafka-topics --bootstrap-server localhost:9092 --list); do kafka-topics --bootstrap-server localhost:9092 --delete --topic $topic; done
    */
    @Bean
    public KafkaAdmin.NewTopics createTopics(){
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("user-position")
                        .replicas(1) // setting replica to 1 because we have only 1 broker
                        .partitions(1)
                        .build(),
                TopicBuilder.name("constructors-position-and-rate")
                        .replicas(1)
                        .partitions(1)
                        .build());
    }
}
