package com.breezzo.proxy.consumer;

import com.breezzo.proxy.client.ServiceDataDeliveryClient;
import com.breezzo.proxy.exception.ApplicationException;
import com.breezzo.proxy.model.ServiceMessageData;
import com.breezzo.proxy.serialization.JavaDeserializer;
import com.breezzo.topic.consumer.ConsumerBase;
import com.breezzo.topic.message.TopicMessage;

import java.util.Optional;


public class ServiceDataDeliveryConsumer extends ConsumerBase implements Runnable, JavaDeserializer {
    private final boolean skipBadMessages;
    private final ServiceDataDeliveryClient client;

    ServiceDataDeliveryConsumer(boolean skipBadMessages, ServiceDataDeliveryClient client) {
        this.skipBadMessages = skipBadMessages;
        this.client = client;
    }

    @Override
    public void run() {
        while (readAndHandleMessage()) ;
    }

    private boolean readAndHandleMessage() {
        final Optional<TopicMessage> topicMessage = readMessage();
        if (topicMessage.isEmpty()) {
            return false;
        }

        return topicMessage
                .flatMap(this::deserializeDataOpt)
                .map(this::deliverMessage)
                .orElseGet(() -> {
                    ackLastReadMessage();
                    return true;
                });
    }

    private boolean deliverMessage(DeserializedMessage message) {
        final boolean messageDelivered =
                client.deliverMessage(message.serviceMessageData, message.topicMessage.getTimestamp());
        if (messageDelivered) {
            ackLastReadMessage();
        }
        return messageDelivered;
    }

    private Optional<DeserializedMessage> deserializeDataOpt(TopicMessage topicMessage) {
        try {
            return Optional.of(new DeserializedMessage(deserialize(topicMessage.getPayload()), topicMessage));
        } catch (Exception e) {
            if (skipBadMessages) {
                return Optional.empty();
            } else {
                throw new ApplicationException(e.getMessage(), e);
            }
        }
    }

    private static class DeserializedMessage {
        final ServiceMessageData serviceMessageData;
        final TopicMessage topicMessage;

        DeserializedMessage(ServiceMessageData serviceMessageData, TopicMessage topicMessage) {
            this.serviceMessageData = serviceMessageData;
            this.topicMessage = topicMessage;
        }
    }
}