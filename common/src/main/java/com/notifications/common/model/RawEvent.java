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
public class RawEvent {

    private String eventId;
    private String eventType;
    private String userId;
    private String tenantId;
    private Map<String, Object> payload;
    @JsonSerialize(using = InstantJsonSerializer.class)
    @JsonDeserialize(using = InstantJsonDeserializer.class)
    private Instant occurredAt;
}
