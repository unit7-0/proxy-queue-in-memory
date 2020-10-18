package com.breezzo.proxy.app;

import com.breezzo.proxy.serialization.JavaSerializer;
import com.breezzo.proxy.model.ServiceMessageData;
import com.breezzo.topic.Topic;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Optional;

public class ProxyHandler implements HttpHandler, JavaSerializer {
    private static final String UNKNOWN_IP = "";
    private final Topic topic;

    public ProxyHandler(Topic topic) {
        this.topic = topic;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (final InputStream is = exchange.getRequestBody();
             final OutputStream os = exchange.getResponseBody()) {
            handlerRequest(exchange, is);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void handlerRequest(HttpExchange exchange, InputStream is) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, 0);
            return;
        }

        final var payload = is.readAllBytes();
        final var remoteIp = getRemoteIp(exchange).orElse(UNKNOWN_IP);
        final var messageData = new ServiceMessageData(payload, remoteIp);
        enqueueMessage(messageData);
        exchange.sendResponseHeaders(200, 0);
    }

    private Optional<String> getRemoteIp(HttpExchange exchange) {
        return Optional.ofNullable(exchange.getRemoteAddress().getAddress())
                       .map(InetAddress::getHostAddress);
    }

    private void enqueueMessage(ServiceMessageData message) {
        final var messageBytes = serialize(message);
        topic.writeMessage(messageBytes);
    }
}
