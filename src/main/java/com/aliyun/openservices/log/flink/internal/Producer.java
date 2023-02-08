package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.TagContent;

import java.util.List;

public interface Producer {

    void open();

    void send(String logstore,
              String topic,
              String source,
              String shardHash,
              List<TagContent> tags,
              List<LogItem> logItems) throws InterruptedException;

    void flush() throws InterruptedException;

    void close();
}
