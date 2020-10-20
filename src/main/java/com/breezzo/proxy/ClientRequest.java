package com.breezzo.proxy;

import java.time.Instant;

public final class ClientRequest {
    private final byte[] payload;
    private final String clientIp;
    private final Instant requestTime;

    public ClientRequest(byte[] payload, String clientIp, Instant requestTime) {
        this.payload = payload;
        this.clientIp = clientIp;
        this.requestTime = requestTime;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getClientIp() {
        return clientIp;
    }

    public Instant getRequestTime() {
        return requestTime;
    }

    @Override
    public String toString() {
        return "ClientRequest{" +
                       "clientIp='" + clientIp + '\'' +
                       ", requestTime=" + requestTime +
                       '}';
    }
}
