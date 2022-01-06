package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogItem;

import java.util.List;

public interface Producer {

    void open();

    void send(String topic,
              String source,
              String shardHash,
              List<LogItem> logItems) throws InterruptedException;

    void flush();

    void close();
}
