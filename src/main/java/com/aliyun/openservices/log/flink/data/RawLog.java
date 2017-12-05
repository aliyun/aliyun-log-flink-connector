package com.aliyun.openservices.log.flink.data;

import org.apache.commons.collections.map.HashedMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RawLog implements Serializable {
    public int time;
    public Map<String, String> contents;

    public RawLog(){
        contents = new HashMap<String, String>();
    }

    public int getTime() {
        return time;
    }

    public Map<String, String> getContents() {
        return contents;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void setContents(Map<String, String> contents) {
        this.contents = contents;
    }

    public void addContent(String key, String value){
        contents.put(key, value);
    }

    @Override
    public String toString() {
        return "RawLog{" +
                "time=" + time +
                ", contents=" + contents +
                '}';
    }
}
