package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.TagContent;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LogGroupKey {

    public static String buildKey(String logstore, String source, String topic, String hashKey, List<TagContent> tags) {
        // TODO What if source topic tag contains $?
        StringBuilder builder = new StringBuilder(logstore);
        for (String it : Arrays.asList(source, topic, hashKey)) {
            builder.append("$");
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
}
