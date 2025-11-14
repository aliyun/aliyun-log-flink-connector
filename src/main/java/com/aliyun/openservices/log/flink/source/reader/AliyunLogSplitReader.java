package com.aliyun.openservices.log.flink.source.reader;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.flink.source.metrics.AliyunLogSourceReaderMetrics;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * SplitReader implementation for reading from Aliyun Log Service splits.
 * Each reader instance handles a single split for parallel fetching performance.
 * This is used by AliyunLogSourceFetcherManager to read data from individual splits.
 */
public class AliyunLogSplitReader implements SplitReader<PullLogsResult, AliyunLogSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogSplitReader.class);

    private final LogClientProxy logClient;
    private final String project;
    private final String consumerGroup;
    private final int fetchSize;
    private final long fetchIntervalMs;

    // Single split being read by this reader
    private AliyunLogSourceSplit activeSplit;
    private volatile boolean running = true;
    private final AliyunLogSourceReaderMetrics sourceReaderMetrics;

    public AliyunLogSplitReader(
            LogClientProxy logClient,
            String project,
            String consumerGroup,
            Properties configProps,
            AliyunLogSourceReaderMetrics aliyunLogSourceReaderMetrics) {
        this.logClient = logClient;
        this.project = project;
        this.consumerGroup = consumerGroup;
        this.fetchSize = LogUtil.getNumberPerFetch(configProps);
        this.fetchIntervalMs = LogUtil.getFetchIntervalMillis(configProps);
        this.sourceReaderMetrics = aliyunLogSourceReaderMetrics;
    }

    @Override
    public RecordsWithSplitIds<PullLogsResult> fetch() throws IOException {
        if (!running || activeSplit == null) {
            return new AliyunLogRecordsWithSplitIds();
        }

        try {
            String logstore = activeSplit.getLogstore();
            int shardId = activeSplit.getShardId();
            String cursor = activeSplit.getNextCursor();
            String stopCursor = activeSplit.getStopCursor();

            PullLogsResult result = logClient.pullLogs(
                    project, logstore, shardId, cursor, stopCursor, fetchSize);

            String nextCursor = result.getNextCursor();
            boolean isFinished = false;
            // Update cursor
            activeSplit.setNextCursor(nextCursor);

            if (result.getCount() > 0) {
                // Check if finished
                if (activeSplit.isReadOnly() && nextCursor.equals(activeSplit.getShardMeta().getEndCursor())) {
                    LOG.debug("Split {} finished (read-only, reached end cursor)", activeSplit.splitId());
                    isFinished = true;
                } else if (nextCursor.equals(stopCursor)) {
                    LOG.debug("Split {} finished (reached stop cursor)", activeSplit.splitId());
                    isFinished = true;
                }
            } else {
                // No data, check if finished
                if (cursor.equals(nextCursor) && activeSplit.isReadOnly()) {
                    LOG.debug("Split {} finished (read-only, no more data)", activeSplit.splitId());
                    isFinished = true;
                }
            }

            // Adjust fetch frequency
            long sleepTime = calculateSleepTime(result.getRawSize());
            if (sleepTime > 0 && result.getCount() == 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }

            if (isFinished) {
                String finishedSplitId = activeSplit.splitId();
                activeSplit = null;
                sourceReaderMetrics.recordFetchEventTimeLag(TimestampAssigner.NO_TIMESTAMP);
                return new AliyunLogRecordsWithSplitIds(Collections.emptyList(),
                        Collections.singleton(finishedSplitId));
            }
            if (result.getCursorTime() > 0) {
                sourceReaderMetrics.recordFetchEventTimeLag(System.currentTimeMillis() - result.getCursorTime());
            }
            if (result.getCount() > 0) {
                return new AliyunLogRecordsWithSplitIds(
                        Collections.singletonList(new RecordWithSplitId<>(result, activeSplit.splitId())),
                        Collections.emptySet());
            }

            return new AliyunLogRecordsWithSplitIds();
        } catch (LogException e) {
            if ("ShardNotExist".equalsIgnoreCase(e.GetErrorCode())) {
                LOG.warn("Shard {} no longer exists, marking split as finished", activeSplit.getShardId());
                String splitId = activeSplit.splitId();
                activeSplit = null;
                return new AliyunLogRecordsWithSplitIds(Collections.emptyList(),
                        Collections.singleton(splitId));
            } else {
                LOG.error("Error fetching logs from split {}", activeSplit.splitId(), e);
                throw new IOException("Error fetching logs", e);
            }
        } catch (Exception e) {
            LOG.error("Unexpected error fetching logs from split {}",
                    activeSplit != null ? activeSplit.splitId() : "unknown", e);
            throw new IOException("Unexpected error fetching logs", e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<AliyunLogSourceSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            SplitsAddition<AliyunLogSourceSplit> addition = (SplitsAddition<AliyunLogSourceSplit>) splitsChange;
            List<AliyunLogSourceSplit> splits = addition.splits();
            if (!splits.isEmpty()) {
                // This reader handles only one split - take the first one
                activeSplit = splits.get(0);
                LOG.debug("Assigned split {} to reader", activeSplit.splitId());
                if (splits.size() > 1) {
                    LOG.warn("Multiple splits provided to single-split reader, only using first: {}",
                            activeSplit.splitId());
                }
            }
        }
    }

    @Override
    public void wakeUp() {
        // No-op, fetch() will be called again
    }

    @Override
    public void close() throws IOException {
        running = false;
        activeSplit = null;
    }

    /**
     * Commit checkpoint for this split's shard.
     * This is called from the fetcher thread when a checkpoint completes.
     *
     * @param split            the shard metadata
     * @param cursor           the cursor to commit
     * @param finishedCallback whether the shard is read-only
     */
    public void notifyCheckpointComplete(AliyunLogSourceSplit split, String cursor, Consumer<Throwable> finishedCallback) {
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            LOG.debug("No consumer group configured, skipping checkpoint commit for shard {}", split.splitId());
            finishedCallback.accept(null);
            return;
        }

        LogstoreShardMeta shardMeta = split.getShardMeta();
        try {
            logClient.updateCheckpoint(
                    project,
                    shardMeta.getLogstore(),
                    consumerGroup,
                    shardMeta.getShardId(),
                    split.isReadOnly(),
                    cursor);
            LOG.debug("Successfully committed checkpoint for shard {}: {}", split.splitId(), cursor);
            finishedCallback.accept(null);
        } catch (Exception e) {
            LOG.error("Failed to commit checkpoint for shard {}: {}", split.splitId(), e.getMessage(), e);
            finishedCallback.accept(e);
        }
    }

    private long calculateSleepTime(int rawSize) {
        long sleepTime = 0;
        if (rawSize <= 1) {
            sleepTime = 1000;
        } else if (rawSize < 256 * 1024) {
            sleepTime = 100;
        }
        return Math.max(sleepTime, fetchIntervalMs);
    }

    /**
     * Wrapper for records with their split IDs.
     */
    private static class RecordWithSplitId<T> {
        private final T record;
        private final String splitId;

        public RecordWithSplitId(T record, String splitId) {
            this.record = record;
            this.splitId = splitId;
        }

        public T getRecord() {
            return record;
        }

        public String getSplitId() {
            return splitId;
        }
    }

    /**
     * Records with split IDs implementation.
     * Note: RecordsWithSplitIds should contain the element type (PullLogsResult), not the split type.
     */
    private static class AliyunLogRecordsWithSplitIds implements RecordsWithSplitIds<PullLogsResult> {
        private final List<RecordWithSplitId<PullLogsResult>> records;
        private final Set<String> finishedSplits;
        private int currentIndex = 0;
        private String currentSplitId;
        private boolean recordConsumed = false;

        public AliyunLogRecordsWithSplitIds() {
            this.records = new ArrayList<>();
            this.finishedSplits = new HashSet<>();
        }

        public AliyunLogRecordsWithSplitIds(List<RecordWithSplitId<PullLogsResult>> records, Set<String> finishedSplits) {
            this.records = records;
            this.finishedSplits = finishedSplits != null ? new HashSet<>(finishedSplits) : new HashSet<>();
        }

        @Override
        public String nextSplit() {
            // Find the next unprocessed record
            if (currentIndex < records.size()) {
                RecordWithSplitId<PullLogsResult> record = records.get(currentIndex);
                currentSplitId = record.getSplitId();
                recordConsumed = false; // Reset for this split
                return currentSplitId;
            }
            return null;
        }

        @Override
        public PullLogsResult nextRecordFromSplit() {
            if (currentIndex < records.size() && !recordConsumed) {
                RecordWithSplitId<PullLogsResult> record = records.get(currentIndex);
                PullLogsResult result = record.getRecord();
                recordConsumed = true; // Mark as consumed
                currentIndex++; // Move to next record for next split
                return result;
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}

