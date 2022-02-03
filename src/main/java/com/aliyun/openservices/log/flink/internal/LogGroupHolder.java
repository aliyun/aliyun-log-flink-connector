package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.TagContent;

import java.util.List;

public class LogGroupHolder {
    private final String source;
    private final String topic;
    private final String hashKey;
    private final List<LogItem> logs;
    private final List<TagContent> tags;
    private int sizeInBytes;

    public LogGroupHolder(String source,
                          String topic,
                          String hashKey,
                          List<TagContent> tags,
                          List<LogItem> logs,
                          int logsSize) {
        this.source = source;
        this.topic = topic == null ? "" : topic;
        this.hashKey = hashKey;
        this.logs = logs;
        this.tags = tags;
        this.sizeInBytes = logsSize;
    }

    public void addLogs(List<LogItem> logs, int sizeInBytes) {
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

    public List<TagContent> getTags() {
        return tags;
    }
}
