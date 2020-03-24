package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ShardConsumer<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    private static final int FORCE_SLEEP_THRESHOLD = 512 * 1024;

    private final LogDataFetcher<T> fetcher;
    private final LogDeserializationSchema<T> deserializer;
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

    ShardConsumer(LogDataFetcher<T> fetcher,
                  LogDeserializationSchema<T> deserializer,
                  int subscribedShardStateIndex,
                  Properties configProps,
                  LogClientProxy logClient,
                  CheckpointCommitter committer) {
        this.fetcher = fetcher;
        this.deserializer = deserializer;
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
            final LogstoreShardMeta shardMeta = state.getShardMeta();
            final int shardId = shardMeta.getShardId();
            String logstore = shardMeta.getLogstore();
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
                        processRecordsAndSaveOffset(response.getLogGroups(), shardMeta, nextCursor);
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
                    adjustFetchFrequency(response.getRawSize(), processingTimeMs);
                }
            }
        } catch (Throwable t) {
            LOG.error("Unexpected error", t);
            fetcher.stopWithError(t);
        }
    }

    private void adjustFetchFrequency(int responseSize, long processingTimeMs) throws InterruptedException {
        long sleepTime = 0;
        if (responseSize == 0) {
            sleepTime = 1000;
        } else if (responseSize < FORCE_SLEEP_THRESHOLD) {
            sleepTime = 200;
        }
        if (sleepTime < fetchIntervalMs) {
            sleepTime = fetchIntervalMs;
        }
        sleepTime -= processingTimeMs;
        if (sleepTime > 0) {
            LOG.debug("Wait {} ms before next fetching", sleepTime);
            Thread.sleep(sleepTime);
        }
    }

    void markAsReadOnly() {
        this.readOnly = true;
    }

    private void processRecordsAndSaveOffset(List<LogGroupData> records, LogstoreShardMeta shard, String nextCursor) {
        final T value = deserializer.deserialize(records);
        long timestamp = System.currentTimeMillis();
        if (!records.isEmpty()) {
            if (records.get(0).GetFastLogGroup().getLogsCount() > 0) {
                long logTimeStamp = records.get(0).GetFastLogGroup().getLogs(0).getTime();
                timestamp = logTimeStamp * 1000;
            }
        }
        fetcher.emitRecordAndUpdateState(value, timestamp, subscribedShardStateIndex, nextCursor);
        if (committer != null) {
            committer.updateCheckpoint(shard, nextCursor, readOnly);
        }
    }

    private boolean isRunning() {
        return !Thread.interrupted();
    }

}
