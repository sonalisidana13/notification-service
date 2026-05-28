package com.notifications.dispatcher;

import java.util.UUID;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.NotificationChannel;
import com.notifications.common.model.NotificationRequest;
import com.notifications.dispatcher.channel.EmailChannelWorker;
import com.notifications.dispatcher.channel.InAppChannelWorker;
import com.notifications.dispatcher.entity.DeliveryLog;
import com.notifications.dispatcher.entity.NotificationEvent;
import com.notifications.dispatcher.ratelimit.SlidingWindowRateLimiter;
import com.notifications.dispatcher.repository.DeliveryLogRepository;
import com.notifications.dispatcher.repository.NotificationEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationDispatcher {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);

    private final SlidingWindowRateLimiter slidingWindowRateLimiter;
    private final EmailChannelWorker emailChannelWorker;
    private final InAppChannelWorker inAppChannelWorker;
    private final NotificationEventRepository notificationEventRepository;
    private final DeliveryLogRepository deliveryLogRepository;
    private final KafkaTemplate<String, NotificationRequest> kafkaTemplate;

    public NotificationDispatcher(
            SlidingWindowRateLimiter slidingWindowRateLimiter,
            EmailChannelWorker emailChannelWorker,
            InAppChannelWorker inAppChannelWorker,
            NotificationEventRepository notificationEventRepository,
            DeliveryLogRepository deliveryLogRepository,
            KafkaTemplate<String, NotificationRequest> kafkaTemplate
    ) {
        this.slidingWindowRateLimiter = slidingWindowRateLimiter;
        this.emailChannelWorker = emailChannelWorker;
        this.inAppChannelWorker = inAppChannelWorker;
        this.notificationEventRepository = notificationEventRepository;
        this.deliveryLogRepository = deliveryLogRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public void dispatch(NotificationRequest req) {
        NotificationEvent notificationEvent = notificationEventRepository.save(buildPendingNotificationEvent(req));

        boolean allowed = slidingWindowRateLimiter.isAllowed(
                req.getUserId(),
                req.getChannel().name(),
                req.getTenantId()
        );

        if (!allowed) {
            notificationEvent.setStatus("THROTTLED");
            notificationEventRepository.save(notificationEvent);
            log.info("Throttled notification for user={}", req.getUserId());
            return;
        }

        try {
            DeliveryResult result = deliver(req);
            notificationEvent.setStatus(result.delivered() ? "SENT" : "FAILED");
            notificationEventRepository.save(notificationEvent);
            saveDeliveryLog(notificationEvent, req.getChannel(), "SUCCESS", null);
        } catch (Exception exception) {
            notificationEvent.setStatus("FAILED");
            notificationEventRepository.save(notificationEvent);
            saveDeliveryLog(notificationEvent, req.getChannel(), "FAIL", exception.getMessage());
            kafkaTemplate.send(KafkaTopics.NOTIFICATION_DLQ, req.getUserId(), req).join();
            log.error("Delivery failed, sent to DLQ: notificationId={} error={}",
                    notificationEvent.getId(),
                    exception.getMessage());
        }
    }

    private DeliveryResult deliver(NotificationRequest req) {
        if (req.getChannel() == NotificationChannel.EMAIL) {
            return emailChannelWorker.deliver(req);
        }
        if (req.getChannel() == NotificationChannel.IN_APP) {
            return inAppChannelWorker.deliver(req);
        }
        throw new IllegalArgumentException("Unsupported channel: " + req.getChannel());
    }

    private NotificationEvent buildPendingNotificationEvent(NotificationRequest req) {
        NotificationEvent notificationEvent = notificationEventRepository.findById(req.getNotificationId())
                .orElseGet(NotificationEvent::new);
        notificationEvent.setId(req.getNotificationId());
        notificationEvent.setSourceEventId(req.getSourceEventId());
        notificationEvent.setUserId(req.getUserId());
        notificationEvent.setTenantId(req.getTenantId());
        notificationEvent.setChannel(req.getChannel());
        notificationEvent.setTemplateKey(req.getTemplateKey());
        notificationEvent.setStatus("PENDING");
        return notificationEvent;
    }

    private void saveDeliveryLog(
            NotificationEvent notificationEvent,
            NotificationChannel channel,
            String status,
            String errorMessage
    ) {
        DeliveryLog deliveryLog = new DeliveryLog();
        deliveryLog.setId(UUID.randomUUID().toString());
        deliveryLog.setNotificationEvent(notificationEvent);
        deliveryLog.setAttemptNumber(1);
        deliveryLog.setChannel(channel);
        deliveryLog.setStatus(status);
        deliveryLog.setErrorMessage(errorMessage);
        deliveryLogRepository.save(deliveryLog);
    }
}
