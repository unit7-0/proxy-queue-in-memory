package com.breezzo.topic.message;

import java.io.Serializable;

public class MessagePayload implements Serializable {
    private static final long serialVersionUID = 1;

    private final byte[] payload;

    public MessagePayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }
}
