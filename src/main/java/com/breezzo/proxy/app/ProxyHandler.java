package com.breezzo.proxy.app;

import com.breezzo.proxy.exception.ApplicationException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Optional;

public class ProxyHandler implements HttpHandler {
    private static final String UNKNOWN_IP = "";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (final InputStream is = exchange.getRequestBody();
             final OutputStream os = exchange.getResponseBody()) {
            handlerRequest(exchange, is);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            throw new ApplicationException(e.getMessage(), e);
        }
    }

    private void handlerRequest(HttpExchange exchange, InputStream is) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, 0);
            return;
        }

        final var payload = is.readAllBytes();
        final var remoteIp = getRemoteIp(exchange).orElse(UNKNOWN_IP);
        // TODO
        exchange.sendResponseHeaders(200, 0);
    }

    private Optional<String> getRemoteIp(HttpExchange exchange) {
        return Optional.ofNullable(exchange.getRemoteAddress().getAddress())
                       .map(InetAddress::getHostAddress);
    }
}
