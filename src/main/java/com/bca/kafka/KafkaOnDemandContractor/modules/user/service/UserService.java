package com.bca.kafka.KafkaOnDemandContractor.modules.user.service;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserRequestDto;

public interface UserService {

    String createUser (UserRequestDto userRequestDto)throws Exception;

}
