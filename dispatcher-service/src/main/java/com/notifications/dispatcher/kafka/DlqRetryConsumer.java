package com.notifications.dispatcher.kafka;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.NotificationRequest;
import com.notifications.dispatcher.MetricsService;
import com.notifications.dispatcher.NotificationDispatcher;
import com.notifications.dispatcher.entity.NotificationEvent;
import com.notifications.dispatcher.repository.DeliveryLogRepository;
import com.notifications.dispatcher.repository.NotificationEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class DlqRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqRetryConsumer.class);
    private static final String FAIL_STATUS = "FAIL";
    private static final String DEAD_STATUS = "DEAD";
    private static final long MAX_RETRY_ATTEMPTS = 5L;
    private static final long MAX_DELAY_SECONDS = 30L;

    private final NotificationDispatcher notificationDispatcher;
    private final NotificationEventRepository notificationEventRepository;
    private final DeliveryLogRepository deliveryLogRepository;
    private final MetricsService metricsService;

    public DlqRetryConsumer(
            NotificationDispatcher notificationDispatcher,
            NotificationEventRepository notificationEventRepository,
            DeliveryLogRepository deliveryLogRepository,
            MetricsService metricsService
    ) {
        this.notificationDispatcher = notificationDispatcher;
        this.notificationEventRepository = notificationEventRepository;
        this.deliveryLogRepository = deliveryLogRepository;
        this.metricsService = metricsService;
    }

    @KafkaListener(
            topics = KafkaTopics.NOTIFICATION_DLQ,
            groupId = "dlq-retry-group",
            containerFactory = "notificationRequestKafkaListenerContainerFactory"
    )
    public void consume(NotificationRequest req, Acknowledgment acknowledgment) {
        long attemptCount = deliveryLogRepository.countByNotificationEventIdAndStatus(
                req.getNotificationId(),
                FAIL_STATUS
        );

        if (attemptCount >= MAX_RETRY_ATTEMPTS) {
            NotificationEvent notificationEvent = notificationEventRepository.findById(req.getNotificationId())
                    .orElseGet(NotificationEvent::new);
            notificationEvent.setId(req.getNotificationId());
            notificationEvent.setSourceEventId(req.getSourceEventId());
            notificationEvent.setUserId(req.getUserId());
            notificationEvent.setTenantId(req.getTenantId());
            notificationEvent.setChannel(req.getChannel());
            notificationEvent.setTemplateKey(req.getTemplateKey());
            notificationEvent.setStatus(DEAD_STATUS);
            notificationEventRepository.save(notificationEvent);
            metricsService.recordDlqDead(req.getChannel().name());
            log.warn("Notification dead-lettered: id={}", req.getNotificationId());
            acknowledgment.acknowledge();
            return;
        }

        long baseDelaySeconds = Math.min(MAX_DELAY_SECONDS, (long) Math.pow(2, attemptCount));
        long delayMs = Duration.ofSeconds(baseDelaySeconds).toMillis();
        // Full jitter spreads retries across the backoff window and prevents a thundering herd
        // when many failed messages become eligible to retry at the same interval.
        long jitterDelayMs = delayMs > 0 ? ThreadLocalRandom.current().nextLong(0, delayMs) : 0;

        try {
            Thread.sleep(jitterDelayMs);
            notificationDispatcher.dispatch(req);
            acknowledgment.acknowledge();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DLQ retry interrupted for notificationId=" + req.getNotificationId(),
                    exception);
        }
    }
}
