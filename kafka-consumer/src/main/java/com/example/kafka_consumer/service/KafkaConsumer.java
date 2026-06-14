package com.example.kafka_consumer.service;


import com.example.kafka_consumer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(
            topics = "kafka-spring-producer",
            groupId = "group_json",
            containerFactory = "userKafkaListenerFactory"
    )
    public void consume(@Payload User user,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        System.out.println("========== MESSAGE RECEIVED ==========");
        System.out.println("Topic      : " + topic);
        System.out.println("Partition  : " + partition);
        System.out.println("Offset     : " + offset);
        System.out.println("User       : " + user);
        System.out.println("======================================");
    }
}