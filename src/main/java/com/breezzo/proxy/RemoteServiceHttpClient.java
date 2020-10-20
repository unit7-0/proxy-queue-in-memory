package com.breezzo.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RemoteServiceHttpClient {
    private static final Logger logger = LoggerFactory.getLogger(RemoteServiceHttpClient.class);

    private final HttpClient httpClient;
    private final URI serviceUri;

    public RemoteServiceHttpClient(URI serviceUri) {
        this.serviceUri = serviceUri;
        this.httpClient = HttpClient.newBuilder()
                                  .version(HttpClient.Version.HTTP_1_1)
                                  .build();
    }

    /**
     * @return true if request was successfully sent false otherwise
     */
    public boolean sendRequest(ClientRequest request) {
        final var httpRequest =
                HttpRequest.newBuilder()
                        .uri(serviceUri)
                        .POST(HttpRequest.BodyPublishers.ofString(formatRequest(request)))
                        .build();
        return trySendRequest(httpRequest);
    }

    private boolean trySendRequest(HttpRequest request) {
        try {
            final var response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            logger.debug("Response code for request: {}: {}", request, response.statusCode());
            return response.statusCode() == 200;
        } catch (Exception e) {
            logger.error("Error occurred while sending request: {}", request, e);
            return false;
        }
    }

    private String formatRequest(ClientRequest request) {
        return String.format(
                "client_ip:%s\ntimestamp:%s\npayload:%s",
                request.getClientIp(),
                request.getRequestTime(),
                new String(request.getPayload())
        );
    }
}
