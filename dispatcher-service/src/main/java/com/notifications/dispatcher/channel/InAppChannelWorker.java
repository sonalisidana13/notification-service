package com.notifications.dispatcher.channel;

import com.notifications.common.model.NotificationRequest;
import com.notifications.dispatcher.DeliveryResult;
import com.notifications.dispatcher.entity.NotificationEvent;
import com.notifications.dispatcher.repository.NotificationEventRepository;
import org.springframework.stereotype.Service;

@Service
public class InAppChannelWorker {

    private final NotificationEventRepository notificationEventRepository;

    public InAppChannelWorker(NotificationEventRepository notificationEventRepository) {
        this.notificationEventRepository = notificationEventRepository;
    }

    public DeliveryResult deliver(NotificationRequest req) {
        NotificationEvent event = notificationEventRepository.findById(req.getNotificationId())
                .orElseGet(NotificationEvent::new);
        event.setId(req.getNotificationId());
        event.setSourceEventId(req.getSourceEventId());
        event.setUserId(req.getUserId());
        event.setTenantId(req.getTenantId());
        event.setChannel(req.getChannel());
        event.setTemplateKey(req.getTemplateKey());
        event.setStatus("SENT");
        notificationEventRepository.save(event);
        return DeliveryResult.success();
    }
}
