package com.breezzo.topic;

public class NamingTools {
    private final String topicPartitionPrefix;
    private final String metaDataName;

    public NamingTools(String topicName, int partitionNumber) {
        this.topicPartitionPrefix = topicName + "/p-" + partitionNumber + "/";
        this.metaDataName = topicPartitionPrefix + "metadata";
    }

    public String dataSegmentName(int segmentNumber) {
        return segmentName(segmentNumber, ".data");
    }

    public String finishedSegmentName(int segmentNumber) {
        return segmentName(segmentNumber, ".finished");
    }

    public String segmentName(int segmentNumber, String suffix) {
        return topicPartitionPrefix + "segment." + segmentNumber + suffix;
    }

    public String metaDataName() {
        return metaDataName;
    }

}
