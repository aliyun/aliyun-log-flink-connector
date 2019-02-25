package com.aliyun.openservices.log.flink.model;

public final class LogShardAssigner {

    private LogShardAssigner() {
    }

    public static boolean isAssigned(LogstoreShardMeta shard, int numParallelSubtasks, int indexOfThisSubtask) {
        int startIndex = ((shard.hashCode() * 31) & 0x7FFFFFFF) % numParallelSubtasks;
        return (startIndex + shard.getShardId()) % numParallelSubtasks == indexOfThisSubtask;
    }

}

