package com.breezzo.proxy.app;

import com.breezzo.proxy.handler.ProxyHandler;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class App {
    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        configureExchanges(server::createContext);
        server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
        server.start();
        System.in.read();
        server.stop(1);
    }

    private static void configureExchanges(BiConsumer<String, HttpHandler> configuration) {
        configuration.accept("/proxy/data", new ProxyHandler());
    }
}
