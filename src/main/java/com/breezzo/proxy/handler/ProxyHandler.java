package com.breezzo.proxy.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.InputStream;
import java.io.OutputStream;

public class ProxyHandler implements HttpHandler {
    public void handle(HttpExchange exchange) {
        try (final InputStream is = exchange.getRequestBody();
             final OutputStream os = exchange.getResponseBody()) {
            final var payload = is.readAllBytes();
            System.out.println(new String(payload));
            exchange.sendResponseHeaders(200, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
