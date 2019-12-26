package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointCommitter extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCommitter.class);

    private volatile boolean running = false;
    private final LogClientProxy logClient;
    private final long commitInterval;
    private final LogDataFetcher fetcher;
    private final Map<Integer, ShardInfo> checkpoints;
    private final String project;
    private final String logstore;
    private final String consumerGroup;

    CheckpointCommitter(LogClientProxy client,
                        long commitInterval,
                        LogDataFetcher fetcher,
                        String project,
                        String logstore,
                        String consumerGroup) {
        this.checkpoints = new ConcurrentHashMap<Integer, ShardInfo>();
        this.logClient = client;
        this.commitInterval = commitInterval;
        this.fetcher = fetcher;
        this.project = project;
        this.logstore = logstore;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void run() {
        if (running) {
            LOG.info("Committer thread already started");
            return;
        }
        running = true;
        try {
            commitCheckpointPeriodic();
        } catch (Throwable t) {
            LOG.error("Error while committing checkpoint", t);
            fetcher.stopWithError(t);
        }
    }

    private void commitCheckpointPeriodic() throws Exception {
        while (running) {
            commitCheckpoints();
            try {
                Thread.sleep(commitInterval);
            } catch (InterruptedException ex) {
                LOG.warn("Interrupt signal received, quiting loop now...");
                break;
            }
        }
    }

    private void commitCheckpoints() throws Exception {
        LOG.debug("Committing checkpoint to remote server");
        for (Integer shard : checkpoints.keySet()) {
            final ShardInfo shardInfo = checkpoints.remove(shard);
            logClient.updateCheckpoint(project, logstore, consumerGroup, shard, shardInfo.readOnly, shardInfo.cursor);
        }
    }

    void updateCheckpoint(Integer shard, String cursor, boolean readOnly) {
        LOG.debug("Updating checkpoint for shard {}, cursor {}", shard, cursor);
        checkpoints.put(shard, new ShardInfo(cursor, readOnly));
    }

    void shutdown() {
        if (!running) {
            return;
        }
        try {
            commitCheckpoints();
        } catch (final Exception ex) {
            LOG.error("Error while committing checkpoint", ex);
        }
        running = false;
    }

    private static class ShardInfo {
        private String cursor;
        private boolean readOnly;

        public ShardInfo(String cursor, boolean readOnly) {
            this.cursor = cursor;
            this.readOnly = readOnly;
        }
    }
}
