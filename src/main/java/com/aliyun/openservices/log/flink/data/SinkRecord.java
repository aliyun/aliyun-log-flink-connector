package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.LogItem;

import java.io.Serializable;

public class SinkRecord implements Serializable {
    private String logstore;
    private String source;
    private String topic = "";
    private String hashKey;
    private LogItem logItem;

    public LogItem getLogItem() {
        return logItem;
    }

    public void setLogItem(LogItem logItem) {
        this.logItem = logItem;
    }

    public String getLogstore() {
        return logstore;
    }

    public void setLogstore(String logstore) {
        this.logstore = logstore;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        if (topic != null) {
            this.topic = topic;
        }
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        if (source != null) {
            this.source = source;
        }
    }

    public String getHashKey() {
        return hashKey;
    }

    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "logstore='" + logstore + '\'' +
                ", source='" + source + '\'' +
                ", topic='" + topic + '\'' +
                ", logItem=" + logItem +
                '}';
    }
}
