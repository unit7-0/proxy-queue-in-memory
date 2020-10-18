package com.breezzo.topic;

import com.breezzo.topic.consumer.Consumer;
import com.breezzo.topic.message.TopicMessage;
import com.breezzo.topic.partition.TopicPartition;
import com.breezzo.topic.partition.TopicPartitionImpl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A topic consists of partitions, due to which parallelism of writing and reading is achieved for one topic.
 * Each partition is stored in its own directory. The partition, in turn, is divided into segments (files)
 * of the same size, due to which small pieces of partitions can be cached in memory, since a producer and
 * consumer can read different segments. It also gives further flexibility, for example, in clearing already
 * read segments of a partition.
 *
 * Any successful write becomes durable because data is saved to persistent storage.
 */
public class TopicImpl implements Topic {
    private final int partitionsCount;
    private final TopicPartition[] partitions;
    private final AtomicInteger partitionWriteCounter = new AtomicInteger();

    public TopicImpl(String name, int defaultPartitionsCount) {
        this.partitionsCount = defaultPartitionsCount;
        this.partitions = new TopicPartition[defaultPartitionsCount];
        for (int i = 0; i < partitionsCount; ++i) {
            partitions[i] = new TopicPartitionImpl(name, i);
        }
    }

    @Override
    public void assignPartitions(final List<? extends Consumer> consumers) {
        if (consumers.size() < 1 || consumers.size() > partitionsCount) {
            throw new IllegalArgumentException(
                    "At least one consumer must be specified, but no more than one for each partition");
        }

        final var consumerToPartitions = new HashMap<Consumer, List<TopicPartition>>();
        for (int partitionNum = 0; partitionNum < partitionsCount; ++partitionNum) {
            final var partition = partitions[partitionNum];
            final var consumer = consumers.get(partitionNum % consumers.size());
            consumerToPartitions.computeIfAbsent(consumer, key -> new ArrayList<>()).add(partition);
        }
        for (Consumer consumer : consumerToPartitions.keySet()) {
            final var partitions = consumerToPartitions.get(consumer);
            consumer.setPartitions(partitions);
        }
    }

    @Override
    public void writeMessage(byte[] payload) {
        final var nextPartitionToWrite = partitions[nextPartitionNumToWrite()];
        final var message = new TopicMessage(payload, Instant.now());
        nextPartitionToWrite.writeMessage(message);
    }

    private int nextPartitionNumToWrite() {
        return Math.abs(partitionWriteCounter.incrementAndGet() % partitionsCount);
    }

    @Override
    public int partitionsCount() {
        return partitionsCount;
    }
}
