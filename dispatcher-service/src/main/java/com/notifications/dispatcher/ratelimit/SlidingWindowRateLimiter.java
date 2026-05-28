package com.notifications.dispatcher.ratelimit;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

@Service
public class SlidingWindowRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRateLimiter.class);

    private final StringRedisTemplate stringRedisTemplate;
    private final RateLimitConfig rateLimitConfig;
    private final RedisScript<Long> rateLimitScript;

    public SlidingWindowRateLimiter(StringRedisTemplate stringRedisTemplate, RateLimitConfig rateLimitConfig) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.rateLimitConfig = rateLimitConfig;
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/rate_limit.lua"));
        script.setResultType(Long.class);
        this.rateLimitScript = script;
    }

    public boolean isAllowed(String userId, String channel, String tenantId) {
        String key = "ratelimit:" + tenantId + ":" + userId + ":" + channel;
        Long result = stringRedisTemplate.execute(
                rateLimitScript,
                List.of(key),
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(rateLimitConfig.getWindowMs()),
                String.valueOf(rateLimitConfig.getMaxPerWindow()),
                UUID.randomUUID().toString()
        );

        boolean allowed = Long.valueOf(1L).equals(result);
        Long currentCount = stringRedisTemplate.opsForZSet().zCard(key);
        log.debug("Rate limit check: key={} allowed={} currentCount={}", key, allowed, currentCount);
        return allowed;
    }
}
