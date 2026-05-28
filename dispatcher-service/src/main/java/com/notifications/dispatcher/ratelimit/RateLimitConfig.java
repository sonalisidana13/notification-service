package com.notifications.dispatcher.ratelimit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimitConfig {

    @Value("${rate.limit.window.ms:60000}")
    private long windowMs;

    @Value("${rate.limit.max.per.window:5}")
    private int maxPerWindow;

    public long getWindowMs() {
        return windowMs;
    }

    public int getMaxPerWindow() {
        return maxPerWindow;
    }
}
