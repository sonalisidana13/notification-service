package com.notifications.dispatcher.repository;

import com.notifications.dispatcher.entity.NotificationEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationEventRepository extends JpaRepository<NotificationEvent, String> {
}
