package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;

import java.io.Serializable;
import java.util.List;

public class ResultHandler<T> implements Serializable {

    private final LogstoreShardMeta shardHandle;
    private final ShardConsumer<T> consumer;

    public ResultHandler(LogstoreShardMeta shardHandle,
                         ShardConsumer<T> consumer) {
        this.shardHandle = shardHandle;
        this.consumer = consumer;
    }

    public void handle(List<LogGroupData> records,
                       String cursor,
                       String nextCursor,
                       int dataRawSize) {
        consumer.processRecords(records, cursor, shardHandle, nextCursor, dataRawSize);
    }
}
