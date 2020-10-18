package com.breezzo.proxy.client;

import com.breezzo.proxy.model.ServiceMessageData;

import java.time.Instant;

public interface ServiceDataDeliveryClient {
    /**
     * @return true if message successfully deliver false otherwise
     */
    boolean deliverMessage(ServiceMessageData messageData, Instant messageTimestamp);
}
