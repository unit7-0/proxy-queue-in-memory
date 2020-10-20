package com.breezzo.proxy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProxyApp {
    private static final Logger logger = LoggerFactory.getLogger(ProxyApp.class);

    private static final int SERVER_PORT = 8080;
    private static final String UNKNOWN_IP = "";
    private static final long ENQUEUE_REQUEST_TIMEOUT_SECONDS = 3;
    private static final URI REMOTE_SERVICE_URI = URI.create("http://localhost:8081");
    private static final int NUMBER_OF_THREADS_FOR_SINGLE_POOL = Runtime.getRuntime().availableProcessors() / 2;

    private static final BlockingQueue<ClientRequest> REQUEST_QUEUE = new ArrayBlockingQueue<>(100_000);

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("Uncaught error occurred", e));

        final var server = startHttpServer();
        final var requestSendingExecutor =
                RequestSendingTask.scheduleTasks(REQUEST_QUEUE, NUMBER_OF_THREADS_FOR_SINGLE_POOL, REMOTE_SERVICE_URI);

        System.in.read();

        server.stop(1);
        requestSendingExecutor.shutdown();
        requestSendingExecutor.awaitTermination(5, TimeUnit.SECONDS);
        requestSendingExecutor.shutdownNow();
    }

    private static HttpServer startHttpServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(SERVER_PORT), 0);
        server.createContext("/proxy/data", ProxyApp::handleClientRequest);
        server.setExecutor(Executors.newFixedThreadPool(NUMBER_OF_THREADS_FOR_SINGLE_POOL));
        server.start();
        return server;
    }

    private static void handleClientRequest(HttpExchange exchange) throws IOException {
        try (final InputStream is = exchange.getRequestBody();
             final OutputStream os = exchange.getResponseBody()) {
            handleClientRequest(exchange, is);
        } catch (Exception e) {
            logger.error("Error occurred while handling request", e);
            sendResponseStatus(exchange, 500);
        }
    }

    private static void handleClientRequest(HttpExchange exchange, InputStream is) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponseStatus(exchange, 405);
            return;
        }

        final var payload = is.readAllBytes();
        final var clientIp = getClientIp(exchange).orElse(UNKNOWN_IP);
        final boolean requestWasEnqueued = enqueueRequest(new ClientRequest(payload, clientIp, Instant.now()));

        if (requestWasEnqueued) {
            sendResponseStatus(exchange, 200);
        } else {
            sendResponseStatus(exchange, 503);
        }
    }

    private static Optional<String> getClientIp(HttpExchange exchange) {
        return Optional.ofNullable(exchange.getRemoteAddress().getAddress())
                       .map(InetAddress::getHostAddress);
    }

    private static boolean enqueueRequest(ClientRequest request) {
        try {
            return REQUEST_QUEUE.offer(request, ENQUEUE_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Thread was interrupted while waiting space for an element", e);
            return false;
        }
    }

    private static void sendResponseStatus(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, 0);
    }
}
