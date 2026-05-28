package com.notifications.dispatcher;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
public class MetricsService {

    private final MeterRegistry meterRegistry;

    public MetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordDispatched(String channel, String status) {
        meterRegistry.counter(
                "notifications.dispatched",
                "channel", channel,
                "status", status
        ).increment();
    }

    public void recordDlqPublished(String channel) {
        meterRegistry.counter(
                "notifications.dlq.published",
                "channel", channel
        ).increment();
    }

    public void recordDlqDead(String channel) {
        meterRegistry.counter(
                "notifications.dlq.dead",
                "channel", channel
        ).increment();
    }

    public void recordDeliveryDuration(String channel, long durationNanos) {
        meterRegistry.timer(
                "notifications.delivery.duration",
                "channel", channel
        ).record(durationNanos, TimeUnit.NANOSECONDS);
    }
}
