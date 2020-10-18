package com.breezzo.proxy.model;

import java.io.Serializable;
import java.util.Arrays;

public class ServiceMessageData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] payload;
    private final String ip;

    public ServiceMessageData(byte[] payload, String ip) {
        this.payload = payload;
        this.ip = ip;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getIp() {
        return ip;
    }

    @Override
    public String toString() {
        return "MessageData{" +
                "payload=" + Arrays.toString(payload) +
                ", ip='" + ip + '\'' +
                '}';
    }
}
