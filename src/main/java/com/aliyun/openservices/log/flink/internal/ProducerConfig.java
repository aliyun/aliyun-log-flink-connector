package com.aliyun.openservices.log.flink.internal;

public class ProducerConfig {

    public static final long DEFAULT_LINGER_MS = 3000;
    public static final int DEFAULT_IO_THREAD_COUNT = Math.max(
            Runtime.getRuntime().availableProcessors() / 2, 1);
    public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 100 * 1024 * 1024;
    public static final int DEFAULT_LOG_GROUP_SIZE = 1024 * 1024;
    public static final int MAX_LOG_GROUP_SIZE = 5 * 1024 * 1024;
    public static final int DEFAULT_MAX_LOG_GROUP_LINES = 2000;
    public static final int MAX_LOG_GROUP_LINES = 40960;

    private int totalSizeInBytes;
    private int logGroupSize;
    private int ioThreadNum;
    private int logGroupMaxLines;
    private long flushInterval;
    private String endpoint;
    private String project;
    private String logstore;
    private String accessKeyId;
    private String accessKeySecret;

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
        this.flushInterval = flushInterval;
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
