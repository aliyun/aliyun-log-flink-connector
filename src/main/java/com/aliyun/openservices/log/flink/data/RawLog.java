package com.aliyun.openservices.log.flink.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RawLog implements Serializable {
    private int time;
    private Map<String, String> contents;
    private Map<String, String> tags;

    public RawLog() {
        contents = new HashMap<String, String>();
        tags = new HashMap<String, String>();
    }

    public int getTime() {
        return time;
    }

    public Map<String, String> getContents() {
        return contents;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void setContents(Map<String, String> contents) {
        this.contents = contents;
    }

    public void addContent(String key, String value) {
        contents.put(key, value);
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void addTags(String key, String value) {
        tags.put(key, value);
    }

    @Override
    public String toString() {
        return "RawLog{" +
                "time=" + time +
                ", contents=" + contents +
                ", tags=" + tags +
                '}';
    }
}
