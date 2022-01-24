package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.TagContent;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LogGroupKey {

    private final String key;

    public LogGroupKey(String source, String topic, String hashKey, List<TagContent> tags) {
        this.key = makeKey(source, topic, hashKey, tags);
    }

    private static String makeKey(String source, String topic, String hashKey, List<TagContent> tags) {
        StringBuilder builder = new StringBuilder();
        boolean isFirst = true;
        for (String it : Arrays.asList(source, topic, hashKey)) {
            if (!isFirst) {
                builder.append("$");
            }
            isFirst = false;
            if (it != null) {
                builder.append(it);
            }
        }
        if (tags != null && !tags.isEmpty()) {
            tags.sort(Comparator.comparing(o -> o.key));
            for (TagContent tag : tags) {
                builder.append("$").append(tag.key).append("#").append(tag.value);
            }
        }
        return builder.toString();
    }

    public String getKey() {
        return key;
    }
}
