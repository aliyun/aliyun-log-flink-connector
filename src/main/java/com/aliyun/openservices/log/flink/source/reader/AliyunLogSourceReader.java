package com.aliyun.openservices.log.flink.source.reader;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointCommitter;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.StartingPosition;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.source.reader.fetcher.AliyunLogSourceFetcherManager;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplitState;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Source reader for Aliyun Log Service that reads data from assigned shards.
 * Uses SourceReaderBase and SplitFetcherManager for better split management.
 */
public class AliyunLogSourceReader<T> extends SourceReaderBase<PullLogsResult, T, AliyunLogSourceSplit, AliyunLogSourceSplitState> implements CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogSourceReader.class);

    private final LogClientProxy logClient;
    private final Properties configProps;
    private final String consumerGroup;
    private final CheckpointMode checkpointMode;
    private final long commitIntervalMs;
    private final StartingPosition startingPosition;
    private CheckpointCommitter checkpointCommitter;

    // Cache checkpoints by checkpointId for synchronous commit
    // Map: checkpointId -> Map of split to cursor
    private final SortedMap<Long, Map<AliyunLogSourceSplit, String>> pendingCheckpoints;

    private final AliyunLogSourceFetcherManager aliyunLogFetcherManager;

    public AliyunLogSourceReader(
            SourceReaderContext context,
            LogClientProxy logClient,
            String project,
            AliyunLogDeserializationSchema<T> deserializer,
            Properties configProps,
            AliyunLogSourceFetcherManager fetcherManager) {
        super(fetcherManager, createRecordEmitter(deserializer), new Configuration(), context);
        this.pendingCheckpoints = Collections.synchronizedSortedMap(new TreeMap<>());
        this.aliyunLogFetcherManager = fetcherManager;
        this.logClient = logClient;
        this.configProps = configProps;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
        this.commitIntervalMs = LogUtil.getCommitIntervalMs(configProps);
        String initialPositionStr = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.startingPosition = StartingPosition.fromString(initialPositionStr);

        LOG.debug("AliyunLogSourceReader initialized: subtask={}, project={}, consumerGroup={}, checkpointMode={}, startingPosition={}",
                context.getIndexOfSubtask(), project, consumerGroup, checkpointMode, startingPosition);

        if (checkpointMode == CheckpointMode.PERIODIC && consumerGroup != null && !consumerGroup.isEmpty()) {
            this.checkpointCommitter = new CheckpointCommitter(
                    logClient, commitIntervalMs, project, consumerGroup);
            this.checkpointCommitter.start();
        }
    }

    private static <T> RecordEmitter<PullLogsResult, T, AliyunLogSourceSplitState> createRecordEmitter(
            AliyunLogDeserializationSchema<T> deserializer) {
        return new AliyunLogRecordEmitter<>(deserializer);
    }

    @Override
    protected void onSplitFinished(Map<String, AliyunLogSourceSplitState> finishedSplitIds) {
        LOG.info("Finished splits: {}", finishedSplitIds.keySet());
    }

    @Override
    protected AliyunLogSourceSplitState initializedState(AliyunLogSourceSplit split) {
        return new AliyunLogSourceSplitState(split);
    }

    @Override
    protected AliyunLogSourceSplit toSplitType(String splitId, AliyunLogSourceSplitState splitState) {
        return splitState.getAliyunLogSourceSplit();
    }

    @Override
    public List<AliyunLogSourceSplit> snapshotState(long checkpointId) {
        LOG.debug("Snapshotting state at checkpoint {}", checkpointId);
        List<AliyunLogSourceSplit> splits = super.snapshotState(checkpointId);
        Map<AliyunLogSourceSplit, String> checkpointMap = new HashMap<>();

        for (AliyunLogSourceSplit split : splits) {
            // Cache checkpoint by checkpointId for synchronous commit
            if (split.getNextCursor() != null && consumerGroup != null && !consumerGroup.isEmpty()) {
                checkpointMap.put(split, split.getNextCursor());
            }

            // For PERIODIC mode, also update the periodic committer
            if (checkpointMode == CheckpointMode.PERIODIC && checkpointCommitter != null
                    && split.getNextCursor() != null) {
                checkpointCommitter.updateCheckpoint(
                        split.getShardMeta(),
                        split.getNextCursor(),
                        split.isReadOnly());
            }
        }

        // Cache checkpoints by checkpointId for ON_CHECKPOINTS mode
        if (checkpointMode == CheckpointMode.ON_CHECKPOINTS && !checkpointMap.isEmpty()) {
            pendingCheckpoints.put(checkpointId, checkpointMap);
            // Clean up old checkpoints to prevent memory leaks (keep only last 10)
            if (pendingCheckpoints.size() > 10) {
                Long oldestCheckpointId = pendingCheckpoints.keySet().stream()
                        .min(Long::compareTo)
                        .orElse(null);
                pendingCheckpoints.remove(oldestCheckpointId);
                LOG.debug("Removed old checkpoint {} from cache to prevent memory leak", oldestCheckpointId);
            }
            LOG.debug("Cached {} checkpoints for checkpoint {}", checkpointMap.size(), checkpointId);
        }

        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (checkpointMode != CheckpointMode.ON_CHECKPOINTS) {
            return;
        }

        if (consumerGroup == null || consumerGroup.isEmpty()) {
            LOG.debug("No consumer group configured, skipping checkpoint commit for checkpoint {}", checkpointId);
            return;
        }
        Map<AliyunLogSourceSplit, String> checkpointMap = pendingCheckpoints.remove(checkpointId);
        if (checkpointMap == null || checkpointMap.isEmpty()) {
            LOG.debug("No cached checkpoints found for checkpoint {}", checkpointId);
            return;
        }

        LOG.info("Committing {} checkpoints via fetcher manager for checkpoint {}", checkpointMap.size(), checkpointId);
        aliyunLogFetcherManager.commitCheckpoints(checkpointMap, result -> {
            if (result.success) {
                LOG.debug("Successfully committed checkpoint for split {}", result.splitId);
                removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            } else {
                LOG.warn(
                        "Failed to commit consumer offsets for checkpoint {}",
                        checkpointId,
                        result.error);
            }
        });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!pendingCheckpoints.isEmpty() && pendingCheckpoints.firstKey() <= checkpointId) {
            pendingCheckpoints.remove(pendingCheckpoints.firstKey());
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // Remove aborted checkpoint from cache
        Map<AliyunLogSourceSplit, String> removed = pendingCheckpoints.remove(checkpointId);
        if (removed != null) {
            LOG.debug("Removed aborted checkpoint {} from cache ({} checkpoints)", checkpointId, removed.size());
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing AliyunLogSourceReader");
        super.close();
        if (checkpointCommitter != null) {
            checkpointCommitter.cancel();
        }
        pendingCheckpoints.clear();
        logClient.close();
    }
}
