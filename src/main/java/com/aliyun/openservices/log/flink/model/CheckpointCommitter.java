package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointCommitter extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCommitter.class);

    private volatile boolean running;
    private final LogClientProxy logClient;
    private final long commitInterval;
    private final LogDataFetcher fetcher;
    private final Map<Integer, String> checkpoints;
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final String consumer;

    CheckpointCommitter(LogClientProxy client,
                        long commitInterval,
                        LogDataFetcher fetcher,
                        String project,
                        String logstore,
                        String consumerGroup,
                        String consumer) {
        checkpoints = new ConcurrentHashMap<Integer, String>();
        this.logClient = client;
        this.commitInterval = commitInterval;
        this.fetcher = fetcher;
        this.project = project;
        this.logstore = logstore;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
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

    private void commitCheckpointPeriodic() {
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

    private void commitCheckpoints() {
        LOG.debug("Committing checkpoint to remote server");
        for (Integer shard : checkpoints.keySet()) {
            final String cursor = checkpoints.remove(shard);
            logClient.updateCheckpoint(project, logstore, consumerGroup, consumer, shard, cursor);
        }
    }

    void updateCheckpoint(Integer shard, String cursor) {
        LOG.debug("Updating checkpoint for shard {}, cursor {}", shard, cursor);
        checkpoints.put(shard, cursor);
    }

    void shutdown() {
        if (!running) {
            return;
        }
        try {
            commitCheckpoints();
        } catch (final Exception ex) {
            LOG.error("Cannot commit checkpoint while shutting down...", ex);
        }
        running = false;
    }
}
