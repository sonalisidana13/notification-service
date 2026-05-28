package com.notifications.dispatcher.channel;

import com.notifications.common.model.NotificationRequest;
import com.notifications.dispatcher.DeliveryResult;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class EmailChannelWorker {

    private final JavaMailSender javaMailSender;

    public EmailChannelWorker(JavaMailSender javaMailSender) {
        this.javaMailSender = javaMailSender;
    }

    public DeliveryResult deliver(NotificationRequest req) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(req.getUserId() + "@example.com");
        message.setSubject("Notification: " + req.getTemplateKey());
        message.setText("Hi " + req.getUserId() + ", template=" + req.getTemplateKey() + " vars=" + req.getTemplateVars());
        javaMailSender.send(message);
        return DeliveryResult.success();
    }
}
