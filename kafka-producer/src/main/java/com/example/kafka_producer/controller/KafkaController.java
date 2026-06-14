package com.example.kafka_producer.controller;

import com.example.kafka_producer.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("v1")
public class KafkaController {

    private final KafkaTemplate<String, User> kafkaTemplate;
    private final String topic;

    public KafkaController(KafkaTemplate<String, User> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping("/publish")
    public String publish(@RequestBody User user) {
        kafkaTemplate.send(topic, user);
        return "Message Published Successfully";
    }
}
