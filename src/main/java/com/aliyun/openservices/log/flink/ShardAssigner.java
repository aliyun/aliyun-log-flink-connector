package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.model.LogstoreShardHandle;

import java.io.Serializable;

public interface ShardAssigner extends Serializable {

    /**
     * Returns the index of the target subtask that a specific shard should be
     * assigned to.
     *
     * @param shardHandle         the shard to determine
     * @param numParallelSubtasks total number of subtasks
     * @return target index, if index falls outside of the range, modulus operation will be applied
     */
    int assign(LogstoreShardHandle shardHandle, int numParallelSubtasks);
}
