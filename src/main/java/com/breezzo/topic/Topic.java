package com.breezzo.topic;

import com.breezzo.topic.consumer.Consumer;

import java.util.List;

public interface Topic {
    void assignPartitions(List<? extends Consumer> consumers);

    void writeMessage(byte[] payload);

    int partitionsCount();
}
