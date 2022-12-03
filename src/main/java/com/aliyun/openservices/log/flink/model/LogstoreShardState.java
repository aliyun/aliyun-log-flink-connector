package com.aliyun.openservices.log.flink.model;

public class LogstoreShardState {
    private LogstoreShardMeta shardMeta;
    private String offset;

    private boolean offsetSaved = false;

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

    boolean isEndReached() {
        return shardMeta.isReadOnly() && offset != null && offset.equals(shardMeta.getEndCursor());
    }

    public boolean isOffsetSaved() {
        return offsetSaved;
    }

    public void setOffsetSaved(boolean offsetSaved) {
        this.offsetSaved = offsetSaved;
    }

    @Override
    public String toString() {
        return "LogstoreShardState{" +
                "shardMeta=" + shardMeta.toString() +
                ", offset='" + offset + '\'' +
                '}';
    }
}
