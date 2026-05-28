package com.notifications.ingest.kafka;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class RawEventProducer {

    private static final Logger log = LoggerFactory.getLogger(RawEventProducer.class);

    private final KafkaTemplate<String, RawEvent> kafkaTemplate;

    public RawEventProducer(KafkaTemplate<String, RawEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, RawEvent>> publish(RawEvent event) {
        CompletableFuture<SendResult<String, RawEvent>> sendFuture =
                kafkaTemplate.send(KafkaTopics.RAW_EVENTS, event.getUserId(), event);

        sendFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to publish raw event with eventId={}", event.getEventId(), throwable);
                return;
            }

            log.info(
                    "Published raw event with eventId={} to partition={}",
                    event.getEventId(),
                    result.getRecordMetadata().partition()
            );
        });

        return sendFuture;
    }
}
