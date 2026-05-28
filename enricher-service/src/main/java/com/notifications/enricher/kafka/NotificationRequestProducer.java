package com.notifications.enricher.kafka;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.NotificationRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class NotificationRequestProducer {

    private final KafkaTemplate<String, NotificationRequest> kafkaTemplate;

    public NotificationRequestProducer(KafkaTemplate<String, NotificationRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, NotificationRequest>> publish(NotificationRequest request) {
        return kafkaTemplate.send(KafkaTopics.NOTIFICATION_REQUESTS, request.getUserId(), request);
    }
}
