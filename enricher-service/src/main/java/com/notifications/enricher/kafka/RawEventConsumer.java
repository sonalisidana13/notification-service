package com.notifications.enricher.kafka;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.NotificationRequest;
import com.notifications.common.model.RawEvent;
import com.notifications.enricher.rules.NotificationRule;
import com.notifications.enricher.rules.NotificationRuleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class RawEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RawEventConsumer.class);

    private final NotificationRuleEngine notificationRuleEngine;
    private final NotificationRequestProducer notificationRequestProducer;

    public RawEventConsumer(
            NotificationRuleEngine notificationRuleEngine,
            NotificationRequestProducer notificationRequestProducer
    ) {
        this.notificationRuleEngine = notificationRuleEngine;
        this.notificationRequestProducer = notificationRequestProducer;
    }

    @KafkaListener(
            topics = KafkaTopics.RAW_EVENTS,
            groupId = "enricher-group",
            containerFactory = "rawEventKafkaListenerContainerFactory"
    )
    public void consume(RawEvent rawEvent, Acknowledgment acknowledgment) {
        List<NotificationRule> rules = notificationRuleEngine.getRulesFor(rawEvent.getEventType());
        List<CompletableFuture<SendResult<String, NotificationRequest>>> publishFutures = rules.stream()
                .map(rule -> buildNotificationRequest(rawEvent, rule))
                .map(notificationRequestProducer::publish)
                .toList();

        try {
            for (CompletableFuture<SendResult<String, NotificationRequest>> publishFuture : publishFutures) {
                try {
                    publishFuture.join();
                } catch (RuntimeException exception) {
                    log.error("Failed to publish notification requests for eventId={}", rawEvent.getEventId(), exception);
                }
            }
        } finally {
            // We still ack on fan-out failure because reprocessing the same source event could duplicate
            // notifications that were already published successfully before the failure occurred.
            acknowledgment.acknowledge();
        }

        log.info("Enriched eventId={} into {} notification requests", rawEvent.getEventId(), rules.size());
    }

    private NotificationRequest buildNotificationRequest(RawEvent rawEvent, NotificationRule rule) {
        return NotificationRequest.builder()
                .notificationId(UUID.randomUUID().toString())
                .sourceEventId(rawEvent.getEventId())
                .userId(rawEvent.getUserId())
                .tenantId(rawEvent.getTenantId())
                .channel(rule.getChannel())
                .templateKey(rule.getTemplateKey())
                .templateVars(toTemplateVars(rawEvent.getPayload()))
                .createdAt(Instant.now())
                .build();
    }

    private Map<String, String> toTemplateVars(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> templateVars = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            templateVars.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }

        return templateVars;
    }
}
