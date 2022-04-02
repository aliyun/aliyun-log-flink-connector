package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;

import java.io.Serializable;

public interface ShardAssigner extends Serializable {

    /**
     * Returns the index of the target subtask that a specific shard should be
     * assigned to.
     *
     * @param shard               the shard to determine
     * @param numParallelSubtasks total number of subtasks
     * @return target index, if index falls outside of the range, modulus operation will be applied
     */
    int assign(LogstoreShardMeta shard, int numParallelSubtasks);
}
