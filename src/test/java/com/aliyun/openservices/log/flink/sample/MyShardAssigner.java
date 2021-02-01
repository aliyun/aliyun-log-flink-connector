package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ShardAssigner;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.flink.util.Consts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyShardAssigner implements ShardAssigner, Serializable {

    private static final long serialVersionUID = -5040045924736116381L;

    private String project;
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private transient Client client;
    private Map<String, List<Integer>> allShards = new ConcurrentHashMap<>();

    public MyShardAssigner(String project,
                           String endpoint,
                           String accessKeyId,
                           String accessKeySecret) {
        this.project = project;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

    private synchronized Client getClient() {
        if (client == null) {
            client = new Client(endpoint, accessKeyId, accessKeySecret);
        }
        return client;
    }

    private synchronized List<Integer> getAllShards(String logstore) {
        List<Integer> shards = allShards.get(logstore);
        if (shards != null) {
            return shards;
        }
        for (int i = 0; i < 5; i++) {
            try {
                List<Shard> shardList = getClient().ListShard(project, logstore).GetShards();
                shards = new ArrayList<>();
                for (Shard shard : shardList) {
                    if (Consts.READONLY_SHARD_STATUS.equalsIgnoreCase(shard.getStatus())) {
                        continue;
                    }
                    shards.add(shard.GetShardId());
                }
                Collections.sort(shards);
                allShards.put(logstore, shards);
                return shards;
            } catch (LogException ex) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException iex) {
                    // ignore
                }
            }
        }
        return Collections.emptyList();
    }

    @Override
    public int assign(LogstoreShardMeta shard, int numParallelSubtasks) {
        if (shard.isReadOnly()) {
            return shard.getShardId() % numParallelSubtasks;
        }
        List<Integer> shards = getAllShards(shard.getLogstore());
        int index = shards.indexOf(shard.getShardId());
        return (index + numParallelSubtasks) % numParallelSubtasks;
    }
}
