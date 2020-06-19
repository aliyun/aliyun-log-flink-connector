package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.ShardAssigner;

public class DefaultShardAssigner implements ShardAssigner {

    @Override
    public int assign(LogstoreShardHandle shard, int numParallelSubtasks) {
        return (Math.abs(shard.hashCode() % numParallelSubtasks));
    }
}
