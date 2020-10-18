package com.breezzo.topic.consumer;

import com.breezzo.topic.partition.TopicPartition;

import java.util.List;

public interface Consumer {
    /**
     * Delegate partitions to consumer for reading.
     */
    void setPartitions(List<TopicPartition> partitions);
}
