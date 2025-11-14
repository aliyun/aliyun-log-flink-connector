package com.aliyun.openservices.log.flink.source.split;

import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * A split representing a shard in Aliyun Log Service.
 * Each split corresponds to one shard that will be consumed by a source reader.
 */
public class AliyunLogSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final LogstoreShardMeta shardMeta;
    private String nextCursor;
    private final String stopCursor;
    private boolean isReadOnly;

    public AliyunLogSourceSplit(LogstoreShardMeta shardMeta, String initialCursor, String stopCursor) {
        this.shardMeta = shardMeta;
        this.splitId = shardMeta.getId();
        this.nextCursor = initialCursor;
        this.stopCursor = stopCursor;
        this.isReadOnly = shardMeta.isReadOnly();
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public LogstoreShardMeta getShardMeta() {
        return shardMeta;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public String getStopCursor() {
        return stopCursor;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }

    public String getLogstore() {
        return shardMeta.getLogstore();
    }

    public int getShardId() {
        return shardMeta.getShardId();
    }

    public boolean isFinished() {
        if (!isReadOnly || nextCursor == null) {
            return false;
        }
        // Check if we've reached the end cursor for read-only shards
        if (nextCursor.equals(shardMeta.getEndCursor())) {
            return true;
        }
        // Check if we've reached the stop cursor
        return stopCursor != null && nextCursor.equals(stopCursor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliyunLogSourceSplit that = (AliyunLogSourceSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    @Override
    public String toString() {
        return "AliyunLogSourceSplit{" +
                "splitId='" + splitId + '\'' +
                ", logstore='" + shardMeta.getLogstore() + '\'' +
                ", shardId=" + shardMeta.getShardId() +
                ", nextCursor='" + nextCursor + '\'' +
                ", isReadOnly=" + isReadOnly +
                '}';
    }
}


