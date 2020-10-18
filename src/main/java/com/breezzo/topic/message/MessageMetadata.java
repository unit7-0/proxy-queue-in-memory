package com.breezzo.topic.message;

import java.io.Serializable;
import java.time.Instant;

public class MessageMetadata implements Serializable {
    private static final long serialVersionUID = 1;

    private final Instant messageTimestamp;

    public MessageMetadata(Instant messageTimestamp) {
        this.messageTimestamp = messageTimestamp;
    }

    public Instant getMessageTimestamp() {
        return messageTimestamp;
    }

    @Override
    public String toString() {
        return "MessageMetadata{" +
                "messageTimestamp=" + messageTimestamp +
                '}';
    }
}
