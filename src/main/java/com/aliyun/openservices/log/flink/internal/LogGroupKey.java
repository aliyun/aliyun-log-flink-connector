package com.aliyun.openservices.log.flink.internal;

public class LogGroupKey {

    private final String key;

    public LogGroupKey(String source, String topic, String hashKey) {
        String key = source == null ? "" : source;
        key += "$" + (topic == null ? "" : topic) + "$";
        if (hashKey != null) {
            key += hashKey;
        }
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
