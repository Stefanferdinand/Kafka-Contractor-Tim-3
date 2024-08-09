package com.bca.kafka.KafkaOnDemandContractor.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "contractor-position-and-rate", groupId = "CG_CONTRACTTOR_USER_RATE")
    public void consume(String message) {
        System.out.println("Analytic Contractor Rate : " + message);
    }
}