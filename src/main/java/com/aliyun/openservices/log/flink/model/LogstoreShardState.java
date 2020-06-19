package com.aliyun.openservices.log.flink.model;

public class LogstoreShardState {
    private LogstoreShardHandle shardHandle;
    private String offset;

    public LogstoreShardState(LogstoreShardHandle shardHandle, String checkpoint) {
        this.shardHandle = shardHandle;
        this.offset = checkpoint;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public LogstoreShardHandle getShardHandle() {
        return shardHandle;
    }

    public void setShardHandle(LogstoreShardHandle shardHandle) {
        this.shardHandle = shardHandle;
    }

    public String getOffset() {
        return offset;
    }

    boolean isIdle() {
        return shardHandle.isReadOnly() && (offset != null && offset.equalsIgnoreCase(shardHandle.getEndCursor()));
    }

    @Override
    public String toString() {
        return "LogstoreShardState{" +
                "shardHandle=" + shardHandle.toString() +
                ", offset='" + offset + '\'' +
                '}';
    }
}
