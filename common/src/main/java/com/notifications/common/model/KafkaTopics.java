package com.notifications.common.model;

public final class KafkaTopics {

    public static final String RAW_EVENTS = "raw-events";
    public static final String NOTIFICATION_REQUESTS = "notification-requests";
    public static final String NOTIFICATION_DLQ = "notification-dlq";

    private KafkaTopics() {
    }
}
