package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogTag;

import java.io.Serializable;
import java.util.List;

public class SourceRecord implements Serializable {
    private String topic;
    private String source;
    private List<FastLogTag> tags;
    private FastLog record;

    public SourceRecord() {
    }

    public SourceRecord(String topic,
                        String source,
                        List<FastLogTag> tags,
                        FastLog record) {
        this.topic = topic;
        this.source = source;
        this.tags = tags;
        this.record = record;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<FastLogTag> getTags() {
        return tags;
    }

    public void setTags(List<FastLogTag> tags) {
        this.tags = tags;
    }

    public FastLog getRecord() {
        return record;
    }

    public void setRecord(FastLog record) {
        this.record = record;
    }

    public long getTimestamp() {
        return record.getTime();
    }
}
