package com.notifications.common.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.notifications.common.model.jackson.InstantJsonDeserializer;
import com.notifications.common.model.jackson.InstantJsonSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationRequest {

    private String notificationId;
    private String sourceEventId;
    private String userId;
    private String tenantId;
    private NotificationChannel channel;
    private String templateKey;
    private Map<String, String> templateVars;
    @JsonSerialize(using = InstantJsonSerializer.class)
    @JsonDeserialize(using = InstantJsonDeserializer.class)
    private Instant createdAt;
}
