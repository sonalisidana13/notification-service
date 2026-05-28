package com.notifications.enricher.rules;

import com.notifications.common.model.NotificationChannel;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NotificationRuleEngine {

    private final List<NotificationRule> rules = List.of(
            new NotificationRule("order.placed", NotificationChannel.EMAIL, "order-confirmation", true),
            new NotificationRule("order.placed", NotificationChannel.IN_APP, "order-placed-inapp", true),
            new NotificationRule("login.failed", NotificationChannel.EMAIL, "security-alert", true),
            new NotificationRule("promotion.created", NotificationChannel.EMAIL, "promo-announcement", true),
            new NotificationRule("promotion.created", NotificationChannel.IN_APP, "promo-inapp", true)
    );

    public List<NotificationRule> getRulesFor(String eventType) {
        return rules.stream()
                .filter(NotificationRule::isEnabled)
                .filter(rule -> rule.getEventType().equals(eventType))
                .toList();
    }
}
