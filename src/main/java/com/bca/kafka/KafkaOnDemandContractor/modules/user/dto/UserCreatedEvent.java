package com.bca.kafka.KafkaOnDemandContractor.modules.user.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
No Args Constructor is required for deserialization purpose, because kafka producer publish an event object, it  will be serialized to byte array
Consumer will need to deserialize the byte array to object, using this class.

The deserialize process will need a no args constructor to create instance of the class before it can populate the fields.
*/
@NoArgsConstructor
@AllArgsConstructor
@Data
public class UserCreatedEvent {
    private String username;
    private String type;
    private double longitude;
    private double latitude;
}

