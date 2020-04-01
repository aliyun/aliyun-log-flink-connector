package com.aliyun.openservices.log.flink.model;

public class LogstoreShardState {
    private LogstoreShardMeta shardMeta;
    private String offset;

    public LogstoreShardState(LogstoreShardMeta shardMeta, String checkpoint) {
        this.shardMeta = shardMeta;
        this.offset = checkpoint;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public void setShardMeta(LogstoreShardMeta shardMeta) {
        this.shardMeta = shardMeta;
    }

    public LogstoreShardMeta getShardMeta() {
        return shardMeta;
    }

    public String getOffset() {
        return offset;
    }

    boolean hasMoreData() {
        return shardMeta.isReadWrite() || !(offset != null && offset.equalsIgnoreCase(shardMeta.getEndCursor()));
    }

    @Override
    public String toString() {
        return "LogstoreShardState{" +
                "shardMeta=" + shardMeta.toString() +
                ", offset='" + offset + '\'' +
                '}';
    }
}
