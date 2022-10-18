package com.aliyun.openservices.log.flink.model;

public class SourceRecord<T> {
    private T record;
    private long timestamp;
    private int subscribedShardStateIndex;
    private String nextCursor;
    private LogstoreShardMeta shard;
    private boolean isReadOnly;

    public SourceRecord(T record, long timestamp, int subscribedShardStateIndex,
                        String nextCursor, LogstoreShardMeta shard, boolean isReadOnly) {
        this.record = record;
        this.timestamp = timestamp;
        this.subscribedShardStateIndex = subscribedShardStateIndex;
        this.nextCursor = nextCursor;
        this.shard = shard;
        this.isReadOnly = isReadOnly;
    }

    public T getRecord() {
        return record;
    }

    public void setRecord(T record) {
        this.record = record;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getSubscribedShardStateIndex() {
        return subscribedShardStateIndex;
    }

    public void setSubscribedShardStateIndex(int subscribedShardStateIndex) {
        this.subscribedShardStateIndex = subscribedShardStateIndex;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public LogstoreShardMeta getShard() {
        return shard;
    }

    public void setShard(LogstoreShardMeta shard) {
        this.shard = shard;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }
}
