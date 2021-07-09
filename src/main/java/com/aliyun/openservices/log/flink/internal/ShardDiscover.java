package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.flink.ShardAssigner;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.flink.util.LogClientProxy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ShardDiscover implements Serializable {
    private final ShardAssigner shardAssigner;
    private final LogClientProxy clientProxy;
    private final String project;
    private final List<String> logstores;
    private final Pattern logstorePattern;
    private final int totalNumberOfSubtasks;
    private final int indexOfThisSubtask;

    public ShardDiscover(ShardAssigner shardAssigner,
                         LogClientProxy clientProxy,
                         String project,
                         List<String> logstores,
                         Pattern logstorePattern,
                         int totalNumberOfSubtasks,
                         int indexOfThisSubtask) {
        this.shardAssigner = shardAssigner;
        this.clientProxy = clientProxy;
        this.project = project;
        this.logstores = logstores;
        this.logstorePattern = logstorePattern;
        this.totalNumberOfSubtasks = totalNumberOfSubtasks;
        this.indexOfThisSubtask = indexOfThisSubtask;
    }

    private List<String> getLogstores() {
        return logstores != null ? logstores : clientProxy.listLogstores(project, logstorePattern);
    }

    public List<LogstoreShardMeta> discoverShards() throws Exception {
        List<String> logstores = getLogstores();
        List<LogstoreShardMeta> shardMetas = new ArrayList<>();
        for (String logstore : logstores) {
            List<Shard> shards = clientProxy.listShards(project, logstore);
            for (Shard shard : shards) {
                LogstoreShardMeta shardMeta = new LogstoreShardMeta(logstore, shard.GetShardId(), shard.getStatus());
                if (shardAssigner.assign(shardMeta, totalNumberOfSubtasks) % totalNumberOfSubtasks == indexOfThisSubtask) {
                    shardMetas.add(shardMeta);
                }
            }
        }
        return shardMetas;
    }
}
