package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ShardConsumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    private static final long ONE_MB_IN_BYTES = 1024 * 1024;
    private static final long TWO_MB_IN_BYTES = 2 * 1024 * 1024;
    private static final long FOUR_MB_IN_BYTES = 4 * 1024 * 1024;

    private final LogDataFetcher fetcher;
    private final int subscribedShardStateIndex;
    private final int fetchSize;
    private final long fetchIntervalMs;
    private final LogClientProxy logClient;
    private final String logProject;
    private String initialPosition;
    private final String defaultPosition;
    private final String consumerGroup;
    private final CheckpointCommitter committer;
    private volatile boolean readOnly = false;

    ShardConsumer(LogDataFetcher fetcher,
                  int subscribedShardStateIndex,
                  Properties configProps,
                  LogClientProxy logClient,
                  CheckpointCommitter committer) {
        this.fetcher = fetcher;
        this.subscribedShardStateIndex = subscribedShardStateIndex;
        // TODO Move configs to a class
        this.fetchSize = LogUtil.getNumberPerFetch(configProps);
        this.fetchIntervalMs = LogUtil.getFetchIntervalMillis(configProps);
        this.logClient = logClient;
        this.committer = committer;
        this.logProject = fetcher.getProject();
        this.initialPosition = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(initialPosition)
                && (consumerGroup == null || consumerGroup.isEmpty())) {
            throw new IllegalArgumentException("Missing parameter: " + ConfigConstants.LOG_CONSUMERGROUP);
        }
        defaultPosition = LogUtil.getDefaultPosition(configProps);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(defaultPosition)) {
            throw new IllegalArgumentException(Consts.LOG_FROM_CHECKPOINT + " cannot be used as default position");
        }
    }

    private String findInitialCursor(String logstore, String position, int shardId) throws Exception {
        String cursor;
        if (Consts.LOG_BEGIN_CURSOR.equals(position)) {
            cursor = logClient.getBeginCursor(logProject, logstore, shardId);
        } else if (Consts.LOG_END_CURSOR.equals(position)) {
            cursor = logClient.getEndCursor(logProject, logstore, shardId);
        } else if (Consts.LOG_FROM_CHECKPOINT.equals(position)) {
            cursor = logClient.fetchCheckpoint(logProject, logstore, consumerGroup, shardId);
            if (cursor == null || cursor.isEmpty()) {
                if (defaultPosition == null || defaultPosition.isEmpty()) {
                    // This should never happen, if no checkpoint found from server side we can
                    // also think the checkpoint is begin cursor. So if no default position is
                    // specified, use the begin cursor as default position is reasonable.
                    throw new RuntimeException("No checkpoint found and default position is not specified");
                }
                LOG.info("No checkpoint available, fallthrough to default position {}", defaultPosition);
                // FIXME change initialPosition as it will be used if pull logs return a InvalidCursor error
                initialPosition = defaultPosition;
                cursor = findInitialCursor(logstore, defaultPosition, shardId);
            }
        } else {
            int timestamp;
            try {
                timestamp = Integer.parseInt(position);
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Unable to parse timestamp which expect a unix timestamp but was: " + position);
            }
            cursor = logClient.getCursorAtTimestamp(logProject, logstore, shardId, timestamp);
        }
        return cursor;
    }

    private String restoreCursorFromStateOrCheckpoint(String logstore,
                                                      String cursor,
                                                      int shardId) throws Exception {
        if (cursor != null) {
            LOG.info("Restored cursor from Flink state: {}, shard: {}", cursor, shardId);
            return cursor;
        }
        cursor = findInitialCursor(logstore, initialPosition, shardId);
        if (cursor == null) {
            throw new RuntimeException("Unable to find the initial cursor from: " + initialPosition);
        }
        return cursor;
    }

    public void run() {
        try {
            LogstoreShardState state = fetcher.getShardState(subscribedShardStateIndex);
            final LogstoreShardHandle shardHandle = state.getShardHandle();
            final int shardId = shardHandle.getShardId();
            String logstore = shardHandle.getLogstore();
            String cursor = restoreCursorFromStateOrCheckpoint(logstore, state.getOffset(), shardId);
            LOG.info("Starting consumer for shard {} with initial cursor {}", shardId, cursor);
            while (isRunning()) {
                PullLogsResponse response;
                long fetchStartTimeMs = System.currentTimeMillis();
                try {
                    response = logClient.pullLogs(logProject, logstore, shardId, cursor, fetchSize);
                } catch (LogException ex) {
                    LOG.warn("Failed to pull logs, message: {}, shard: {}", ex.GetErrorMessage(), shardId);
                    String errorCode = ex.GetErrorCode();
                    if ("ShardNotExist".equals(errorCode)) {
                        // The shard has been deleted
                        LOG.warn("The shard {} already not exist, project {} logstore {}", shardId, logProject, logstore);
                        break;
                    }
                    if ("InvalidCursor".equalsIgnoreCase(errorCode)
                            && Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(initialPosition)) {
                        LOG.warn("Got invalid cursor error, start from default position {}", defaultPosition);
                        cursor = findInitialCursor(logstore, defaultPosition, shardId);
                        continue;
                    }
                    throw ex;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Fetch request cost {} ms", System.currentTimeMillis() - fetchStartTimeMs);
                }
                if (response != null) {
                    long processingTimeMs;
                    String nextCursor = response.getNextCursor();
                    if (response.getCount() > 0) {
                        long processingStartTimeMs = System.currentTimeMillis();
                        processRecordsAndSaveOffset(response.getLogGroups(), shardHandle, nextCursor);
                        processingTimeMs = System.currentTimeMillis() - processingStartTimeMs;
                        LOG.debug("Processing records of shard {} cost {} ms.", shardId, processingTimeMs);
                        cursor = nextCursor;
                    } else {
                        processingTimeMs = 0;
                        LOG.debug("No records of shard {} has been responded.", shardId);
                        if (cursor.equals(nextCursor) && readOnly) {
                            LOG.info("Shard {} is finished", shardId);
                            break;
                        }
                    }
                    adjustFetchFrequency(response.getRawSize(), response.getCount(), processingTimeMs);
                }
            }
        } catch (Throwable t) {
            LOG.error("Unexpected error", t);
            fetcher.stopWithError(t);
        }
    }

    private static long getThrottlingInterval(int lastFetchSize,
                                              int lastFetchCount,
                                              int batchGetSize) {
        if (lastFetchSize < ONE_MB_IN_BYTES && lastFetchCount < batchGetSize) {
            return 500;
        } else if (lastFetchSize < TWO_MB_IN_BYTES && lastFetchCount < batchGetSize) {
            return 200;
        } else if (lastFetchSize < FOUR_MB_IN_BYTES && lastFetchCount < batchGetSize) {
            return 50;
        } else {
            return 0;
        }
    }

    private void adjustFetchFrequency(int responseSize,
                                      int responseCount,
                                      long processingTimeMs) throws InterruptedException {
        long sleepTimeInMs = getThrottlingInterval(responseSize, responseCount, fetchSize);
        if (sleepTimeInMs < fetchIntervalMs) {
            sleepTimeInMs = fetchIntervalMs;
        }
        sleepTimeInMs -= processingTimeMs;
        if (sleepTimeInMs > 0) {
            LOG.debug("Wait {} ms before next fetching", sleepTimeInMs);
            Thread.sleep(sleepTimeInMs);
        }
    }

    void markAsReadOnly() {
        this.readOnly = true;
    }

    private void processRecordsAndSaveOffset(List<LogGroupData> allLogGroups, LogstoreShardHandle shard, String nextCursor) {
        List<SourceRecord> records = new ArrayList<>();
        if (allLogGroups != null) {
            for (LogGroupData logGroup : allLogGroups) {
                FastLogGroup fastLogGroup = logGroup.GetFastLogGroup();
                int n = fastLogGroup.getLogsCount();
                int numOfTags = fastLogGroup.getLogTagsCount();
                List<FastLogTag> tags = new ArrayList<>(numOfTags);
                for (int j = 0; j < numOfTags; j++) {
                    tags.add(fastLogGroup.getLogTags(j));
                }
                for (int i = 0; i < n; i++) {
                    SourceRecord record = new SourceRecord(
                            fastLogGroup.getTopic(),
                            fastLogGroup.getSource(),
                            tags,
                            fastLogGroup.getLogs(i));
                    records.add(record);
                }
            }
        }
        fetcher.emitRecordAndUpdateState(records, subscribedShardStateIndex, nextCursor);
        if (committer != null) {
            committer.updateCheckpoint(shard, nextCursor, readOnly);
        }
    }

    private boolean isRunning() {
        return !Thread.interrupted();
    }

}
