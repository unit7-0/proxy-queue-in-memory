package com.breezzo.proxy.app;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class ProxyApp {
    private static final Logger logger = LoggerFactory.getLogger(ProxyApp.class);

    public static void main(String[] args) throws IOException {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("Uncaught error occurred", e));

        HttpServer server = initializeHttpServer();

        System.in.read();
        server.stop(1);
    }

    private static HttpServer initializeHttpServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        configureExchanges(server::createContext);
        server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2));
        server.start();
        return server;
    }

    private static void configureExchanges(BiConsumer<String, HttpHandler> configuration) {
        configuration.accept("/proxy/data", new ProxyHandler());
    }
}
