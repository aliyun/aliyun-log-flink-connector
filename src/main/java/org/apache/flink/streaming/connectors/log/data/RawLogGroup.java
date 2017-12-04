package org.apache.flink.streaming.connectors.log.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawLogGroup implements Serializable {
    public String source;
    public String topic = "";
    public Map<String, String> tags;
    public List<RawLog> logs;

    public RawLogGroup()
    {
        tags = new HashMap<String, String>();
        logs = new ArrayList<RawLog>();
    }

    public void addTag(String key, String value){
        tags.put(key, value);
    }

    public void addLog(RawLog log){
        logs.add(log);
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
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

    public List<RawLog> getLogs() {
        return logs;
    }

    public void setLogs(List<RawLog> logs) {
        this.logs = logs;
    }

    @Override
    public String toString() {
        StringBuilder strb = new StringBuilder();
        if(logs.size() > 0){
            strb.append(logs.get(0).toString());
        }
        return "RawLogGroup{" +
                "source='" + source + '\'' +
                ", topic='" + topic + '\'' +
                ", tags=" + tags +
                ", logs=" + strb +
                '}';
    }
}
