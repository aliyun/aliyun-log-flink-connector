package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.Consts;

public class LogstoreShardState {
    private LogstoreShardMeta shardMeta;
    private String lastConsumerCursor;

    public LogstoreShardState(LogstoreShardMeta shardMeta, String lastConsumerCursor){
        this.shardMeta = shardMeta;
        this.lastConsumerCursor = lastConsumerCursor;
    }

    public void setLastConsumerCursor(String lastConsumerCursor) {
        this.lastConsumerCursor = lastConsumerCursor;
    }

    public void setShardMeta(LogstoreShardMeta shardMeta) {
        this.shardMeta = shardMeta;
    }

    public LogstoreShardMeta getShardMeta() {
        return shardMeta;
    }

    public String getLastConsumerCursor() {
        return lastConsumerCursor;
    }

    public boolean hasMoreData(){
        if(shardMeta.getShardStatus().compareToIgnoreCase(Consts.READWRITE_SHARD_STATUS) == 0) return true;
        else if(shardMeta.getShardStatus().compareToIgnoreCase(Consts.READONLY_SHARD_STATUS) == 0){
            if(lastConsumerCursor == null || shardMeta.getEndCursor() == null) return true;
            else if(lastConsumerCursor.compareTo(shardMeta.getEndCursor()) == 0) return false;
            else return true;
        }
        else return false;
    }

    @Override
    public String toString() {
        return "LogstoreShardState{" +
                "shardMeta=" + shardMeta.toString() +
                ", lastConsumerCursor='" + lastConsumerCursor + '\'' +
                '}';
    }
}
