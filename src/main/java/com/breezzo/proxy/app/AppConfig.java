package com.breezzo.proxy.app;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AppConfig {
    public static final String APP_FOLDER = "/home/breezzo/projects/proxy-queue/app/";
    public static final String SERVICE_URI = "http://localhost:8081";
    public static final String PROXY_TOPIC_NAME = "back-topic";
    public static final int DEFAULT_TOPIC_PARTITIONS_COUNT = 32;

    public static Path resolve(String name) {
        return Paths.get(APP_FOLDER).resolve(name);
    }
}
