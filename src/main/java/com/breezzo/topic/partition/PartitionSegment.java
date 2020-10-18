package com.breezzo.topic.partition;

import com.breezzo.proxy.app.AppConfig;
import com.breezzo.proxy.exception.ApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PartitionSegment {
    private static final Logger logger = LoggerFactory.getLogger(PartitionSegment.class);

    private SoftReference<MappedByteBuffer> bufferRef;
    private final String segmentName;
    private final int bufferSize;

    public PartitionSegment(MappedByteBuffer buffer, String segmentName, int bufferSize) {
        this.bufferRef = new SoftReference<>(buffer);
        this.segmentName = segmentName;
        this.bufferSize = bufferSize;
    }

    public MappedByteBuffer getBuffer() {
        var buffer = bufferRef.get();
        if (buffer == null) {
            buffer = loadBuffer(segmentName, bufferSize);
            bufferRef = new SoftReference<>(buffer);
        }
        return buffer;
    }

    public static PartitionSegment load(String segmentName, int bufferSize) {
        return new PartitionSegment(loadBuffer(segmentName, bufferSize), segmentName, bufferSize);
    }

    public static void finish(String segmentName) {
        try {
            Files.createFile(AppConfig.resolve(segmentName));
        } catch (IOException e) {
            throw new ApplicationException(e.getMessage(), e);
        }
    }

    public static boolean isFinished(String segmentName) {
        return Files.exists(AppConfig.resolve(segmentName));
    }

    private static MappedByteBuffer loadBuffer(String segmentName, int bufferSize) {
        try {
            Path segmentPath = AppConfig.resolve(segmentName);
            logger.debug("Loading partition segment from: {}", segmentPath);
            Files.createDirectories(segmentPath.getParent());
            return FileChannel
                    .open(
                            segmentPath,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE
                    )
                    .map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
        } catch (Exception e) {
            throw new ApplicationException(e.getMessage(), e);
        }
    }
}
