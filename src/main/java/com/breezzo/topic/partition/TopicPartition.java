package com.breezzo.topic.partition;

import com.breezzo.topic.message.TopicMessage;

import java.util.Optional;

public interface TopicPartition {
    void writeMessage(TopicMessage topicMessage);

    /**
     * May be invoked many times resulting the same message until {@link #ackLastReadMessage()} method called.
     */
    Optional<TopicMessage> readMessage();

    void ackLastReadMessage();
}
