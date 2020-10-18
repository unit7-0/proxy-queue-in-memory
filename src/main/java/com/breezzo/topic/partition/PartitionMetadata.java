package com.breezzo.topic.partition;

import com.breezzo.proxy.exception.ApplicationException;
import com.breezzo.proxy.app.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Metadata for each partition in topic, contains basic information for now:
 * <ul>
 *     <li>{@link #nextWriteOffset} - offset at which next message should be write</li>
 *     <li>{@link #nextReadOffset} - offset at which next message should be read</li>
 *     <li>{@link #maxBytesPerSegment} - max segment size in bytes, upon reaching which a new segment will be created</li>
 * </ul>
 */
public class PartitionMetadata {
    private static final Logger logger = LoggerFactory.getLogger(PartitionMetadata.class);

    /**
     * size of fields: nextWriteOffset + nextReadOffset + maxBytesPerSegment
     */
    private static final int METADATA_SIZE = 20;

    private long nextWriteOffset;
    private long nextReadOffset;
    private int lastReadMessageLength;
    private final int maxBytesPerSegment;
    private final MappedByteBuffer buffer;

    private PartitionMetadata(
            int nextWriteOffset,
            int nextReadOffset,
            int maxBytesPerSegment,
            MappedByteBuffer buffer
    ) {
        this.nextWriteOffset = nextWriteOffset;
        this.nextReadOffset = nextReadOffset;
        this.maxBytesPerSegment = maxBytesPerSegment;
        this.lastReadMessageLength = 0;
        this.buffer = buffer;
    }

    public long getNextWriteOffset() {
        return nextWriteOffset;
    }

    public long getNextReadOffset() {
        return nextReadOffset;
    }

    public int getLastReadMessageLength() {
        return lastReadMessageLength;
    }

    public void setNextWriteOffset(long nextWriteOffset) {
        this.nextWriteOffset = nextWriteOffset;
    }

    public void setNextReadOffset(long nextReadOffset) {
        this.nextReadOffset = nextReadOffset;
    }

    public void setLastReadMessageLength(int lastReadMessageLength) {
        this.lastReadMessageLength = lastReadMessageLength;
    }

    public int getMaxBytesPerSegment() {
        return maxBytesPerSegment;
    }

    public void flush() {
        buffer.position(4)
              .putLong(nextWriteOffset)
              .putLong(nextReadOffset);
        buffer.force();
    }

    public static PartitionMetadata load(String name, int defaultMaxBytesPerSegment) {
        try {
            return loadMetadata(name, defaultMaxBytesPerSegment);
        } catch (Exception e) {
            throw new ApplicationException(e.getMessage(), e);
        }
    }

    private static PartitionMetadata loadMetadata(String name, int defaultMaxBytesPerSegment) throws IOException {
        Path metadataPath = Paths.get(AppConfig.APP_FOLDER).resolve(name);
        logger.debug("Loading partition metadata from: {}", metadataPath);
        Files.createDirectories(metadataPath.getParent());
        final FileChannel channel = FileChannel.open(
                metadataPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        final var buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, METADATA_SIZE);
        final int readMaxBytesPerSegment = buffer.getInt();
        final int maxBytesPerSegment = readMaxBytesPerSegment == 0 ? defaultMaxBytesPerSegment : readMaxBytesPerSegment;
        final int lastWriteOffset = buffer.getInt();
        final int lastReadOffset = buffer.getInt();

        if (readMaxBytesPerSegment == 0) {
            buffer.position(0).putInt(maxBytesPerSegment);
            buffer.force();
        }
        return new PartitionMetadata(lastWriteOffset, lastReadOffset, maxBytesPerSegment, buffer);
    }
}
