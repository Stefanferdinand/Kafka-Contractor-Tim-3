package com.bca.kafka.KafkaOnDemandContractor.modules.user.service;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserRequestDto;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

@Service
@AllArgsConstructor
public class UserServiceImpl implements UserService {

    private final KafkaTemplate<String,UserCreatedEvent> kafkaTemplate;
    private final Logger LOGGER = Logger.getLogger(UserServiceImpl.class.getName());

    @Override
    public String createUser(UserRequestDto userRequestDto) throws Exception{
        UserCreatedEvent userCreatedEvent = new UserCreatedEvent(userRequestDto.getUsername(),
                userRequestDto.getType(), userRequestDto.getLongitude(), userRequestDto.getLatitude());


        LOGGER.info("Before Sending User to kafka");


        SendResult<String,UserCreatedEvent> result = kafkaTemplate.send("user-position",
                userCreatedEvent.getUsername(), userCreatedEvent).get(); // sync behavior, wait until the message is sent to the server waiting for acknowledgement


        LOGGER.info("Partition:  "  + result.getRecordMetadata().partition() );
        LOGGER.info("Topic " + result.getRecordMetadata().topic());
        LOGGER.info("Offset " + result.getRecordMetadata().offset());
        LOGGER.info("Timestamp " + result.getRecordMetadata().timestamp());
        LOGGER.info("After Sending User to kafka");

        return  "Username:" + userCreatedEvent.getUsername() + " created succesfully";
    }

    private void sendMessageWithAsync(UserCreatedEvent userCreatedEvent) {
        CompletableFuture<SendResult<String,UserCreatedEvent>> future = kafkaTemplate.send("user-position",
                userCreatedEvent.getUsername(), userCreatedEvent); // async behavior, doesn't wait until the message is sent to the server

        future.whenComplete((result,exception)->{
            if(exception!=null)
            {
                LOGGER.info("Error occured while sending message to kafka"+ exception.getMessage());
            }
            else
            {
                LOGGER.info("Message sent to kafka successfully " +  result.getRecordMetadata());
            }
        });
        future.join();// wait until the message is sent to the server same like await in javascript
        LOGGER.info("Return user  successfully");
    }
}
