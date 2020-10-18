package com.breezzo.proxy.app;

import com.breezzo.proxy.consumer.ServiceDataDeliveryInitializer;
import com.breezzo.proxy.client.HttpDataDeliveryClient;
import com.breezzo.topic.Topic;
import com.breezzo.topic.TopicImpl;
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
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("Uncaught error occurred", e);
        });

        final var topic = new TopicImpl(AppConfig.PROXY_TOPIC_NAME, AppConfig.DEFAULT_TOPIC_PARTITIONS_COUNT);
        HttpServer server = initializeHttpServer(topic);

        final var deliveryClient = new HttpDataDeliveryClient(AppConfig.SERVICE_URI);
        final var consumer = new ServiceDataDeliveryInitializer(topic);
        consumer.consumeTopic(deliveryClient);

        System.in.read();
        server.stop(1);
    }

    private static HttpServer initializeHttpServer(TopicImpl topic) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        configureExchanges(server::createContext, topic);
        server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2));
        server.start();
        return server;
    }

    private static void configureExchanges(BiConsumer<String, HttpHandler> configuration, Topic topic) {
        configuration.accept("/proxy/data", new ProxyHandler(topic));
    }
}
