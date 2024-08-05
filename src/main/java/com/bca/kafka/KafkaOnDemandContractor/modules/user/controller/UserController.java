package com.bca.kafka.KafkaOnDemandContractor.modules.user.controller;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserRequestDto;
import com.bca.kafka.KafkaOnDemandContractor.modules.user.service.UserService;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("/user")
@AllArgsConstructor
public class UserController {

        private final UserService userService;
        private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
        @PostMapping
        public ResponseEntity<Object>createUsers(@RequestBody UserRequestDto userRequestDto) {
            UserCreatedEvent createdUser;
            try {
                //produce
                createdUser = userService.createUser(userRequestDto);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(),e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorMessage(new Date(), e.getMessage(), "/users"));
            }

            return ResponseEntity.ok(createdUser);
        }
}
