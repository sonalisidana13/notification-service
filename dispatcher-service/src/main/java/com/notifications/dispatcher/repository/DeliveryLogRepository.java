package com.notifications.dispatcher.repository;

import com.notifications.dispatcher.entity.DeliveryLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeliveryLogRepository extends JpaRepository<DeliveryLog, String> {

    long countByNotificationEventId(String notificationId);

    long countByNotificationEventIdAndStatus(String notificationId, String status);
}
