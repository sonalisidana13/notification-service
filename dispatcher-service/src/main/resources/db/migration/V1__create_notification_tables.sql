-- Author: Sonali Sidana
-- Purpose: Initial migration to create tables for notification events and delivery logs.

CREATE TYPE notification_channel AS ENUM ('EMAIL', 'IN_APP');

CREATE TABLE notification_events (
    id VARCHAR(36) PRIMARY KEY,
    source_event_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    channel notification_channel NOT NULL,
    template_key VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_log (
    id VARCHAR(36) PRIMARY KEY,
    notification_id VARCHAR(36) NOT NULL REFERENCES notification_events(id),
    attempt_number INTEGER NOT NULL DEFAULT 1,
    channel notification_channel NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    attempted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notification_user ON notification_events(user_id);
CREATE INDEX idx_delivery_notification ON delivery_log(notification_id);
