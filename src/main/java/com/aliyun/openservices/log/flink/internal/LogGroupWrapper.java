package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogItem;

import java.util.List;

public class LogGroupWrapper {

    private final String source;
    private final String topic;
    private final String hashKey;
    private final List<LogItem> logs;
    private int sizeInBytes;

    public LogGroupWrapper(String source, String topic, String hashKey, List<LogItem> logs, int bytes) {
        this.source = source;
        this.topic = topic == null ? "" : topic;
        this.hashKey = hashKey;
        this.logs = logs;
        this.sizeInBytes = bytes;
    }

    public void add(List<LogItem> logs, int sizeInBytes) {
        this.logs.addAll(logs);
        this.sizeInBytes += sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public String getSource() {
        return source;
    }

    public String getTopic() {
        return topic;
    }

    public String getHashKey() {
        return hashKey;
    }

    public List<LogItem> getLogs() {
        return logs;
    }
}
