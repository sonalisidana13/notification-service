package com.notificationservice.dispatcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
        "com.notificationservice.dispatcher",
        "com.notifications.dispatcher",
        "com.notifications.common"
})
public class DispatcherServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DispatcherServiceApplication.class, args);
    }
}
