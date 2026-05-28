package com.notifications.dispatcher.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.mail.MailSenderAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(classes = SlidingWindowRateLimiterTest.TestApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class SlidingWindowRateLimiterTest {

    private static final String TENANT_ID = "tenant-1";
    private static final String USER_ID = "user-1";
    private static final String EMAIL_CHANNEL = "email";
    private static final String SMS_CHANNEL = "sms";
    private static final long WINDOW_MS = 200L;

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
            .withExposedPorts(6379);

    @Autowired
    private SlidingWindowRateLimiter slidingWindowRateLimiter;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
        registry.add("rate.limit.window.ms", () -> WINDOW_MS);
        registry.add("rate.limit.max.per.window", () -> 5);
    }

    @BeforeEach
    void clearRedis() {
        stringRedisTemplate.execute((RedisCallback<Void>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    void shouldAllowFiveRequestsAndRejectTheSixth() {
        for (int attempt = 0; attempt < 5; attempt++) {
            assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isFalse();
    }

    @Test
    void shouldUseSeparateKeysPerChannel() {
        for (int attempt = 0; attempt < 5; attempt++) {
            assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isFalse();
        assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, SMS_CHANNEL, TENANT_ID)).isTrue();
    }

    @Test
    void shouldAllowAgainAfterWindowExpires() throws InterruptedException {
        for (int attempt = 0; attempt < 5; attempt++) {
            assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isFalse();

        Thread.sleep(WINDOW_MS + 150L);

        assertThat(slidingWindowRateLimiter.isAllowed(USER_ID, EMAIL_CHANNEL, TENANT_ID)).isTrue();
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            FlywayAutoConfiguration.class,
            MailSenderAutoConfiguration.class,
            KafkaAutoConfiguration.class
    })
    @Import({RateLimitConfig.class, SlidingWindowRateLimiter.class})
    static class TestApplication {
    }
}
