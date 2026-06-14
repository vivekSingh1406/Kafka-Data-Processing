package com.example.kafka_consumer.service;


import com.example.kafka_consumer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
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

        log.info("message received topic={}, partition={}, offset={}, message={}", topic, partition, offset, user);
    }

    /**
     * Called when number of failure exceeds configured maxFailure number.
     * Note that we are using String type to receive the message instead of POJO. This can be useful when
     * the failure is due to deserialization which Spring won't be able to construct POJO at all.
     */
    @KafkaListener(topics = "kafka-spring-producer", containerFactory = "userKafkaListenerFactory")
    public void dltListen(final String message) {
        log.info("received from DeadLetterTopic, failed to consume message, message={}", message);
    }
}