package com.breezzo.topic.message;

import java.time.Instant;

public class TopicMessage {
    private final byte[] payload;
    private final Instant timestamp;

    public TopicMessage(byte[] payload, Instant timestamp) {
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "TopicMessage{" +
                "timestamp=" + timestamp +
                '}';
    }
}
