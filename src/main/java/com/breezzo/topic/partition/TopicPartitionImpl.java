package com.breezzo.topic.partition;

import com.breezzo.proxy.serialization.JavaDeserializer;
import com.breezzo.proxy.serialization.JavaSerializer;
import com.breezzo.topic.NamingTools;
import com.breezzo.topic.message.MessageMetadata;
import com.breezzo.topic.message.MessagePayload;
import com.breezzo.topic.message.TopicMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class TopicPartitionImpl implements JavaSerializer, JavaDeserializer, TopicPartition {
    private static final int LENGTH_FIELDS_BYTES = 6;
    private static final int DEFAULT_MAX_BYTES_PER_SEGMENT = 128 * 1024 * 1024;

    private final NamingTools namingTools;
    private final PartitionMetadata partitionMetadata;
    private final Map<Integer, PartitionSegment> segments = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public TopicPartitionImpl(String topicName, int partitionNumber) {
        this(topicName, partitionNumber, DEFAULT_MAX_BYTES_PER_SEGMENT);
    }

    public TopicPartitionImpl(String topicName, int partitionNumber, int maxBytesPerSegment) {
        this.namingTools = new NamingTools(topicName, partitionNumber);
        this.partitionMetadata = PartitionMetadata.load(namingTools.metaDataName(), maxBytesPerSegment);
    }

    @Override
    public void writeMessage(TopicMessage topicMessage) {
        writeLocking(() -> {
            final long probablyNextWriteOffset = partitionMetadata.getNextWriteOffset();

            final var metadata = new MessageMetadata(topicMessage.getTimestamp());
            final var payload = new MessagePayload(topicMessage.getPayload());

            final var metadataBytes = serialize(metadata);
            final var payloadBytes = serialize(payload);

            final int messageSize = metadataBytes.length + payloadBytes.length + LENGTH_FIELDS_BYTES;

            final var segmentWrite = acquireSegmentWrite(probablyNextWriteOffset, messageSize);
            final var nextWriteOffset = probablyNextWriteOffset + segmentWrite.lastSegmentSkippedBytes;
            final var segment = segmentWrite.segment;
            final var segmentBuffer = segment.getBuffer();

            segmentBuffer.position((int) (nextWriteOffset % maxBytesPerSegment()));
            segmentBuffer.putShort((short) metadataBytes.length);
            segmentBuffer.putInt(payloadBytes.length);
            segmentBuffer.put(metadataBytes);
            segmentBuffer.put(payloadBytes);

            partitionMetadata.setNextWriteOffset(nextWriteOffset + messageSize);
            partitionMetadata.flush();

            segmentBuffer.force();
        });
    }

    @Override
    public Optional<TopicMessage> readMessage() {
        return readLocking(() -> {
            final long probablyNextReadOffset = partitionMetadata.getNextReadOffset();
            final long nextWriteOffset = partitionMetadata.getNextWriteOffset();

            if (probablyNextReadOffset >= nextWriteOffset) {
                return Optional.empty();
            }

            final var segmentRead = acquireSegmentRead(probablyNextReadOffset);
            final long nextReadOffset = probablyNextReadOffset + segmentRead.lastSegmentSkippedBytes;
            final var segment = segmentRead.segment;
            final var segmentBuffer = segment.getBuffer();

            segmentBuffer.position((int) (nextReadOffset % maxBytesPerSegment()));

            final short metaDataLength = segmentBuffer.getShort();
            if (metaDataLength == 0) {
                return Optional.empty();
            }

            final int payloadLength = segmentBuffer.getInt();

            final byte[] metaDataBytes = new byte[metaDataLength];
            final byte[] payloadBytes = new byte[payloadLength];

            segmentBuffer.get(metaDataBytes, 0, metaDataLength);
            segmentBuffer.get(payloadBytes, 0, payloadLength);

            final MessageMetadata metadata = deserialize(metaDataBytes);
            final MessagePayload payload = deserialize(payloadBytes);

            partitionMetadata.setLastReadMessageLength(
                    metaDataLength + payloadLength + LENGTH_FIELDS_BYTES + segmentRead.lastSegmentSkippedBytes
            );

            return Optional.of(new TopicMessage(payload.getPayload(), metadata.getMessageTimestamp()));
        });
    }

    @Override
    public void ackLastReadMessage() {
        writeLocking(() -> {
            partitionMetadata.setNextReadOffset(
                    partitionMetadata.getNextReadOffset() + partitionMetadata.getLastReadMessageLength());
            partitionMetadata.setLastReadMessageLength(0);
            partitionMetadata.flush();
        });
    }

    private SegmentWithSkippedBytes acquireSegmentWrite(long offset, int messageSize) {
        final int segmentNumber;
        final int maxBytesPerSegment = maxBytesPerSegment();
        int lastSegmentSkippedBytes = 0;
        final int remainSegmentBytes = remainSegmentBytes(offset);

        if (remainSegmentBytes >= messageSize) {
            segmentNumber = (int) (offset / maxBytesPerSegment);
        } else {
            final int toFinishSegmentNumber = (int) (offset / maxBytesPerSegment);
            finishSegment(toFinishSegmentNumber);
            segmentNumber = toFinishSegmentNumber + 1;
            lastSegmentSkippedBytes = remainSegmentBytes;
        }

        final var segment = segments.computeIfAbsent(
                segmentNumber,
                key -> PartitionSegment.load(namingTools.dataSegmentName(segmentNumber), maxBytesPerSegment)
        );

        return new SegmentWithSkippedBytes(segment, lastSegmentSkippedBytes);
    }

    private SegmentWithSkippedBytes acquireSegmentRead(long offset) {
        final int maxBytesPerSegment = maxBytesPerSegment();
        final int probablySegmentNumber = (int) (offset / maxBytesPerSegment);
        final int segmentNumber;
        final int lastSegmentSkippedBytes;

        if (PartitionSegment.isFinished(namingTools.finishedSegmentName(probablySegmentNumber))) {
            segmentNumber = probablySegmentNumber + 1;
            lastSegmentSkippedBytes = remainSegmentBytes(offset);
        } else {
            segmentNumber = probablySegmentNumber;
            lastSegmentSkippedBytes = 0;
        }

        final var segment = segments.computeIfAbsent(
                segmentNumber,
                key -> PartitionSegment.load(namingTools.dataSegmentName(segmentNumber), maxBytesPerSegment)
        );

        return new SegmentWithSkippedBytes(segment, lastSegmentSkippedBytes);
    }

    private int remainSegmentBytes(long offset) {
        final int maxBytesPerSegment = maxBytesPerSegment();
        return (int) (maxBytesPerSegment - offset % maxBytesPerSegment);
    }

    private void writeLocking(Runnable action) {
        writeLock.lock();
        try {
            action.run();
        } finally {
            writeLock.unlock();
        }
    }

    private <T> T readLocking(Supplier<T> supplier) {
        readLock.lock();
        try {
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Marks segment as finished so no writes will be executed event if some bytes in segment is free.
     */
    private void finishSegment(int segmentNumber) {
        PartitionSegment.finish(namingTools.finishedSegmentName(segmentNumber));
    }

    private int maxBytesPerSegment() {
        return partitionMetadata.getMaxBytesPerSegment();
    }

    private static class SegmentWithSkippedBytes {
        final PartitionSegment segment;
        final int lastSegmentSkippedBytes;

        SegmentWithSkippedBytes(PartitionSegment segment, int lastSegmentSkippedBytes) {
            this.segment = segment;
            this.lastSegmentSkippedBytes = lastSegmentSkippedBytes;
        }
    }
}
