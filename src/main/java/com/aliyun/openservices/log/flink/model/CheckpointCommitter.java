package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CheckpointCommitter extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCommitter.class);

    private volatile boolean running = false;
    private final LogClientProxy logClient;
    private final long commitInterval;
    private Map<String, ShardInfo> checkpoints;
    private final String project;
    private final String consumerGroup;
    private final Lock lock;
    private final CompletableFuture<Void> cancelFuture = new CompletableFuture<>();

    CheckpointCommitter(LogClientProxy client,
                        long commitInterval,
                        String project,
                        String consumerGroup) {
        this.checkpoints = new HashMap<>();
        this.logClient = client;
        this.commitInterval = commitInterval;
        this.project = project;
        this.consumerGroup = consumerGroup;
        this.lock = new ReentrantLock();
    }

    @Override
    public void run() {
        if (running) {
            LOG.info("Committer thread already started");
            return;
        }
        running = true;
        commitCheckpointPeriodic();
    }

    private void commitCheckpointPeriodic() {
        while (running) {
            try {
                commitCheckpoints();
            } catch (Throwable t) {
                LOG.error("Error while committing checkpoint", t);
            }
            try {
                cancelFuture.get(commitInterval, TimeUnit.MILLISECONDS);
            } catch (TimeoutException iex) {
                // timeout is expected when fetcher is not cancelled
            } catch (Exception ignore) {
                // should never happen
            }
        }
    }

    private void commitCheckpoints() throws Exception {
        LOG.debug("Committing checkpoint to remote server");
        Map<String, ShardInfo> backup = null;
        lock.lock();
        try {
            if (checkpoints.isEmpty()) {
                return;
            }
            backup = checkpoints;
            checkpoints = new HashMap<>();
        } finally {
            lock.unlock();
        }
        for (Map.Entry<String, ShardInfo> entry : backup.entrySet()) {
            final ShardInfo shardInfo = entry.getValue();
            logClient.updateCheckpoint(project,
                    shardInfo.logstore,
                    consumerGroup,
                    shardInfo.shard,
                    shardInfo.cursor);
        }
    }

    void updateCheckpoint(LogstoreShardMeta shard, String cursor, boolean readOnly) {
        LOG.debug("Updating checkpoint for shard {}, cursor {}", shard, cursor);
        lock.lock();
        try {
            checkpoints.put(shard.getId(), new ShardInfo(cursor, readOnly, shard.getLogstore(), shard.getShardId()));
        } finally {
            lock.unlock();
        }
    }

    void cancel() {
        if (!running) {
            return;
        }
        running = false;
        try {
            commitCheckpoints();
        } catch (final Exception ex) {
            LOG.error("Error while committing checkpoint", ex);
        }
        cancelFuture.complete(null);
    }

    private static class ShardInfo {
        private final String cursor;
        private final boolean readOnly;
        private final String logstore;
        private final int shard;

        ShardInfo(String cursor, boolean readOnly, String logstore, int shard) {
            this.cursor = cursor;
            this.readOnly = readOnly;
            this.logstore = logstore;
            this.shard = shard;
        }
    }
}
