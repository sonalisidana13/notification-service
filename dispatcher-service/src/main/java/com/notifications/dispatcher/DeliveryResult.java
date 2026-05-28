package com.notifications.dispatcher;

public record DeliveryResult(boolean delivered, String errorMessage) {

    public static DeliveryResult success() {
        return new DeliveryResult(true, null);
    }

    public static DeliveryResult failure(String error) {
        return new DeliveryResult(false, error);
    }
}
