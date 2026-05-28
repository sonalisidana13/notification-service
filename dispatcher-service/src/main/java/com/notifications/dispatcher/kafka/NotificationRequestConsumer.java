package com.notifications.dispatcher.kafka;

import com.notifications.common.model.KafkaTopics;
import com.notifications.common.model.NotificationRequest;
import com.notifications.dispatcher.NotificationDispatcher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class NotificationRequestConsumer {

    private final NotificationDispatcher notificationDispatcher;

    public NotificationRequestConsumer(NotificationDispatcher notificationDispatcher) {
        this.notificationDispatcher = notificationDispatcher;
    }

    @KafkaListener(
            topics = KafkaTopics.NOTIFICATION_REQUESTS,
            groupId = "dispatcher-group",
            containerFactory = "notificationRequestKafkaListenerContainerFactory"
    )
    public void consume(NotificationRequest req, Acknowledgment acknowledgment) {
        notificationDispatcher.dispatch(req);
        acknowledgment.acknowledge();
    }
}
