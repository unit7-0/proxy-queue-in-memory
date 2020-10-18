package com.breezzo.proxy.consumer;

import com.breezzo.proxy.client.ServiceDataDeliveryClient;
import com.breezzo.topic.Topic;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ServiceDataDeliveryInitializer {
    private final Topic topic;
    private final ScheduledExecutorService executorService;

    public ServiceDataDeliveryInitializer(Topic topic) {
        this.topic = topic;
        this.executorService =
                Executors.newScheduledThreadPool(
                        Math.min(topic.partitionsCount(), Runtime.getRuntime().availableProcessors() / 2)
                );
    }

    public void consumeTopic(ServiceDataDeliveryClient deliveryClient) {
        final var consumers = new ServiceDataDeliveryConsumer[topic.partitionsCount()];
        fillArray(consumers, () -> new ServiceDataDeliveryConsumer(true, deliveryClient));

        final var consumerList = List.of(consumers);

        topic.assignPartitions(consumerList);

        for (ServiceDataDeliveryConsumer consumer : consumerList) {
            executorService.scheduleAtFixedRate(consumer, 0, 1, TimeUnit.SECONDS);
        }
    }

    private void fillArray(ServiceDataDeliveryConsumer[] array, Supplier<ServiceDataDeliveryConsumer> constructor) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = constructor.get();
        }
    }
}
