package com.bca.kafka.KafkaOnDemandContractor.modules.user.dto;

import lombok.Data;

@Data
public class UserRequestDto {

    private String username;
    private String type; // contractor or customer
    private double longitude;
    private double latitude;

}
