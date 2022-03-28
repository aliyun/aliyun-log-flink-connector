package com.aliyun.openservices.log.flink.internal;

public class ProducerConfig {

    public static final long DEFAULT_LINGER_MS = 3000;
    public static final int DEFAULT_IO_THREAD_COUNT = Math.max(
            Runtime.getRuntime().availableProcessors() / 2, 1);
    public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 200 * 1024 * 1024;
    public static final int DEFAULT_LOG_GROUP_SIZE = 3 * 1024 * 1024;
    public static final int MAX_LOG_GROUP_SIZE = 8 * 1024 * 1024;
    public static final int DEFAULT_MAX_LOG_GROUP_LINES = 5000;
    public static final int MAX_LOG_GROUP_LINES = 40960;
    public static final int DEFAULT_PRODUCER_QUEUE_SIZE = 4096;
    public static final int DEFAULT_BUCKETS = 64;
    public static final boolean DEFAULT_ADJUST_SHARD_HASH = true;

    private int totalSizeInBytes;
    private int logGroupSize;
    private int ioThreadNum;
    private int logGroupMaxLines;
    private int producerQueueSize;
    private long flushInterval;
    private String endpoint;
    private String project;
    private String logstore;
    private String accessKeyId;
    private String accessKeySecret;
    private boolean adjustShardHash;
    private int buckets;

    public int getLogGroupSize() {
        return logGroupSize;
    }

    public void setLogGroupSize(int logGroupSize) {
        if (logGroupSize <= 0 || logGroupSize >= MAX_LOG_GROUP_SIZE) {
            throw new IllegalArgumentException("logGroupSize must be within range (0, " + MAX_LOG_GROUP_SIZE + "]");
        }
        this.logGroupSize = logGroupSize;
    }

    public int getLogGroupMaxLines() {
        return logGroupMaxLines;
    }

    public void setLogGroupMaxLines(int logGroupMaxLines) {
        if (logGroupMaxLines <= 0 || logGroupMaxLines > MAX_LOG_GROUP_LINES) {
            throw new IllegalArgumentException("logGroupMaxLines must be within range (0, " + MAX_LOG_GROUP_LINES + "]");
        }
        this.logGroupMaxLines = logGroupMaxLines;
    }

    public int getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void setTotalSizeInBytes(int totalSizeInBytes) {
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(long flushInterval) {
        if (flushInterval < 100) {
            throw new IllegalArgumentException("flushInterval must be > 100");
        }
        this.flushInterval = flushInterval;
    }

    public int getProducerQueueSize() {
        return producerQueueSize;
    }

    public void setProducerQueueSize(int producerQueueSize) {
        if (producerQueueSize <= 0) {
            throw new IllegalArgumentException("producerQueueSize must be > 0");
        }
        this.producerQueueSize = producerQueueSize;
    }

    public int getIoThreadNum() {
        return ioThreadNum;
    }

    public void setIoThreadNum(int ioThreadNum) {
        if (ioThreadNum <= 0) {
            throw new IllegalArgumentException("ioThreadNum must be positive");
        }
        this.ioThreadNum = ioThreadNum;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint cannot be null");
        }
        this.endpoint = endpoint;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        if (project == null) {
            throw new IllegalArgumentException("project cannot be null");
        }
        this.project = project;
    }

    public String getLogstore() {
        return logstore;
    }

    public void setLogstore(String logstore) {
        if (logstore == null) {
            throw new IllegalArgumentException("logstore cannot be null");
        }
        this.logstore = logstore;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        if (accessKeyId == null) {
            throw new IllegalArgumentException("accessKeyId cannot be null");
        }
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        if (accessKeySecret == null) {
            throw new IllegalArgumentException("accessKeySecret cannot be null");
        }
        this.accessKeySecret = accessKeySecret;
    }

    public boolean isAdjustShardHash() {
        return adjustShardHash;
    }

    public void setAdjustShardHash(boolean adjustShardHash) {
        this.adjustShardHash = adjustShardHash;
    }

    public int getBuckets() {
        return buckets;
    }

    public void setBuckets(int buckets) {
        if (!validateBuckets(buckets)) {
            throw new IllegalArgumentException("buckets must be a power of 2, but was " + buckets);
        }
        this.buckets = buckets;
    }

    private static boolean validateBuckets(int number) {
        return number > 0 && ((number & (number - 1)) == 0);
    }

    @Override
    public String toString() {
        return "ProducerConfig{" +
                "totalSizeInBytes=" + totalSizeInBytes +
                ", logGroupSize=" + logGroupSize +
                ", ioThreadNum=" + ioThreadNum +
                ", logGroupMaxLines=" + logGroupMaxLines +
                ", flushInterval=" + flushInterval +
                ", endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", logstore='" + logstore + '\'' +
                '}';
    }
}
