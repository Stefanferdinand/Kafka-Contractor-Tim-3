package com.bca.kafka.KafkaOnDemandContractor.modules.user.service;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserRequestDto;

public interface UserService {

    UserCreatedEvent createUser (UserRequestDto userRequestDto)throws Exception;



}
