package com.notifications.enricher.rules;

import com.notifications.common.model.NotificationChannel;

public class NotificationRule {

    private final String eventType;
    private final NotificationChannel channel;
    private final String templateKey;
    private final boolean enabled;

    public NotificationRule(String eventType, NotificationChannel channel, String templateKey, boolean enabled) {
        this.eventType = eventType;
        this.channel = channel;
        this.templateKey = templateKey;
        this.enabled = enabled;
    }

    public String getEventType() {
        return eventType;
    }

    public NotificationChannel getChannel() {
        return channel;
    }

    public String getTemplateKey() {
        return templateKey;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
