package com.breezzo.proxy.client;

import com.breezzo.proxy.model.ServiceMessageData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

public class HttpDataDeliveryClient implements ServiceDataDeliveryClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpDataDeliveryClient.class);

    private final HttpClient httpClient;
    private final URI serviceUri;

    public HttpDataDeliveryClient(String serviceUri) {
        this.serviceUri = URI.create(serviceUri);
        this.httpClient = HttpClient
                .newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    @Override
    public boolean deliverMessage(ServiceMessageData messageData, Instant messageTimestamp) {
        final var request = HttpRequest.newBuilder()
                                       .uri(serviceUri)
                                       .POST(HttpRequest.BodyPublishers
                                                     .ofString(messageToBody(messageData, messageTimestamp)))
                                       .build();
        return sendRequest(request);
    }

    private boolean sendRequest(HttpRequest request) {
        try {
            final var response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            logger.debug("Response code for request: {}: {}", request, response.statusCode());
            return response.statusCode() == 200;
        } catch (Exception e) {
            logger.error("Error occurred while sending request: {}", request, e);
            return false;
        }
    }

    private String messageToBody(ServiceMessageData messageData, Instant messageTimestamp) {
        return String.format("ip:%s\ntimestamp:%s\npayload:%s", messageData.getIp(), messageTimestamp,
                      new String(messageData.getPayload()));
    }
}
