package com.bca.kafka.KafkaOnDemandContractor.config;

import com.bca.kafka.KafkaOnDemandContractor.modules.user.dto.UserCreatedEvent;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class CustomSerde implements Serde {
    @Override
    public Serializer serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer deserializer() {
        return new JsonDeserializer(UserCreatedEvent.class);
    }
}
