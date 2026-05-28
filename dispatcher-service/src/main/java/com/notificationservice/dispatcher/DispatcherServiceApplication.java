package com.notificationservice.dispatcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {
        "com.notificationservice.dispatcher",
        "com.notifications.dispatcher",
        "com.notifications.common"
})
@EntityScan(basePackages = "com.notifications.dispatcher.entity")
@EnableJpaRepositories(basePackages = "com.notifications.dispatcher.repository")
public class DispatcherServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DispatcherServiceApplication.class, args);
    }
}
