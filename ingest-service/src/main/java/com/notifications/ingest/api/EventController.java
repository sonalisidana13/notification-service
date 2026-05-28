package com.notifications.ingest.api;

import com.notifications.common.model.RawEvent;
import com.notifications.ingest.kafka.RawEventProducer;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final RawEventProducer rawEventProducer;

    public EventController(RawEventProducer rawEventProducer) {
        this.rawEventProducer = rawEventProducer;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> ingestEvent(@Valid @RequestBody IngestEventRequest request) {
        String eventId = UUID.randomUUID().toString();
        RawEvent event = RawEvent.builder()
                .eventId(eventId)
                .eventType(request.getEventType())
                .userId(request.getUserId())
                .tenantId(request.getTenantId())
                .payload(request.getPayload())
                .occurredAt(Instant.now())
                .build();

        rawEventProducer.publish(event);

        return ResponseEntity.accepted()
                .body(Map.of("eventId", eventId, "status", "ACCEPTED"));
    }
}
