package com.breezzo.topic.consumer;

import com.breezzo.topic.message.TopicMessage;
import com.breezzo.topic.partition.TopicPartition;

import java.util.List;
import java.util.Optional;

/**
 * Basic consumer implementation, subclasses should use {@link #readMessage} method to read messages and
 * {@link #ackLastReadMessage()} to acknowledge last read message(move to next partition offset).
 */
public abstract class ConsumerBase implements Consumer {
    private volatile List<TopicPartition> partitions = null;
    private int lastReadPartition = -1;

    protected Optional<TopicMessage> readMessage() {
        final var partitionToRead = partitions.get(updateLastReadPartition());
        return partitionToRead.readMessage();
    }

    private int updateLastReadPartition() {
        lastReadPartition = (lastReadPartition + 1) % partitions.size();
        return lastReadPartition;
    }

    protected void ackLastReadMessage() {
        final var partition = partitions.get(lastReadPartition);
        partition.ackLastReadMessage();
    }

    @Override
    public void setPartitions(List<TopicPartition> partitions) {
        if (this.partitions != null || partitions == null) {
            throw new IllegalArgumentException("Partitions initialization possible only once with not null value");
        }
        this.partitions = partitions;
    }
}
