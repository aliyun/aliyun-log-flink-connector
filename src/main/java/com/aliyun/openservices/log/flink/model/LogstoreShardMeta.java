package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.Consts;

import java.io.Serializable;
import java.util.Objects;

public class LogstoreShardMeta implements Serializable {
    private String logstore;
    private int shardId;
    private String shardStatus;
    private String endCursor;

    // DO NOT remove
    public LogstoreShardMeta() {
    }

    public LogstoreShardMeta(String logstore, int shardId, String shardStatus) {
        this.logstore = logstore;
        this.shardId = shardId;
        this.shardStatus = shardStatus;
    }

    public String getLogstore() {
        return logstore;
    }

    public void setLogstore(String logstore) {
        this.logstore = logstore;
    }

    public int getShardId() {
        return shardId;
    }

    public String getShardStatus() {
        return shardStatus;
    }

    public String getEndCursor() {
        return endCursor;
    }

    boolean needSetEndCursor() {
        return isReadOnly() && endCursor == null;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public void setShardStatus(String shardStatus) {
        this.shardStatus = shardStatus;
    }

    public void setEndCursor(String endCursor) {
        this.endCursor = endCursor;
    }

    public boolean isReadOnly() {
        return Consts.READONLY_SHARD_STATUS.equalsIgnoreCase(this.shardStatus);
    }

    public boolean isReadWrite() {
        return Consts.READWRITE_SHARD_STATUS.equalsIgnoreCase(this.shardStatus);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogstoreShardMeta shardMeta = (LogstoreShardMeta) o;

        if (shardId != shardMeta.shardId) return false;
        return Objects.equals(logstore, shardMeta.logstore);
    }

    @Override
    public int hashCode() {
        int result = logstore != null ? logstore.hashCode() : 0;
        result = 31 * result + shardId;
        return result;
    }

    @Override
    public String toString() {
        return "LogstoreShardMeta{" +
                "logstore='" + logstore + '\'' +
                ", shardId=" + shardId +
                ", shardStatus='" + shardStatus + '\'' +
                ", endCursor='" + endCursor + '\'' +
                '}';
    }
}
