package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.Consts;

import java.io.Serializable;

public class LogstoreShardMeta implements Serializable {
    private int shardId;
    private String beginHashKey;
    private String endHashKey;
    //readonly, readwrite
    private String shardStatus;
    private String endCursor;

    // DONOT remove
    public LogstoreShardMeta() {
    }

    public LogstoreShardMeta(int shardId, String beginHashKey, String endHashKey, String shardStatus) {
        this.shardId = shardId;
        this.beginHashKey = beginHashKey;
        this.endHashKey = endHashKey;
        this.shardStatus = shardStatus;
    }

    public int getShardId() {
        return shardId;
    }

    public String getEndHashKey() {
        return endHashKey;
    }

    public String getBeginHashKey() {
        return beginHashKey;
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

    public void setBeginHashKey(String beginHashKey) {
        this.beginHashKey = beginHashKey;
    }

    public void setShardStatus(String shardStatus) {
        this.shardStatus = shardStatus;
    }

    public void setEndHashKey(String endHashKey) {
        this.endHashKey = endHashKey;
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

        LogstoreShardMeta that = (LogstoreShardMeta) o;

        if (shardId != that.shardId) return false;
        if (!beginHashKey.equals(that.beginHashKey)) return false;
        if (!endHashKey.equals(that.endHashKey)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = shardId;
        result = 31 * result + beginHashKey.hashCode();
        result = 31 * result + endHashKey.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LogstoreShardMeta{" +
                "shardId=" + shardId +
                ", beginHashKey='" + beginHashKey + '\'' +
                ", endHashKey='" + endHashKey + '\'' +
                ", shardStatus='" + shardStatus + '\'' +
                ", endCursor='" + endCursor + '\'' +
                '}';
    }
}
