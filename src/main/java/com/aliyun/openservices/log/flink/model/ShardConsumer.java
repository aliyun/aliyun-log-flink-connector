package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.apache.flink.util.PropertiesUtil;
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
    private final String logStore;
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
        this.fetchSize = getNumberPerFetch(configProps);
        this.fetchIntervalMs = getFetchIntervalMillis(configProps);
        this.logClient = logClient;
        this.committer = committer;
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.initialPosition = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(initialPosition)
                && (consumerGroup == null || consumerGroup.isEmpty())) {
            throw new IllegalArgumentException("Missing parameter: " + ConfigConstants.LOG_CONSUMERGROUP);
        }
        defaultPosition = getDefaultPosition(configProps);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(defaultPosition)) {
            throw new IllegalArgumentException("Cannot use " + Consts.LOG_FROM_CHECKPOINT + " as the default position");
        }
    }

    private String findInitialCursor(String position, int shardId) throws Exception {
        String cursor;
        if (Consts.LOG_BEGIN_CURSOR.equals(position)) {
            cursor = logClient.getBeginCursor(logProject, logStore, shardId);
        } else if (Consts.LOG_END_CURSOR.equals(position)) {
            cursor = logClient.getEndCursor(logProject, logStore, shardId);
        } else if (Consts.LOG_FROM_CHECKPOINT.equals(position)) {
            // Fetch checkpoint first, if no checkpoint found, fallthrough default position.
            cursor = logClient.fetchCheckpoint(logProject, logStore, consumerGroup, shardId);
            if (cursor == null || cursor.isEmpty()) {
                if (defaultPosition == null || defaultPosition.isEmpty()) {
                    throw new RuntimeException("No checkpoint found");
                }
                LOG.info("No checkpoint available, fallthrough to default position {}", defaultPosition);
                // FIXME change initialPosition as it will be used if pull logs return a InvalidCursor error
                initialPosition = defaultPosition;
                return findInitialCursor(defaultPosition, shardId);
            }
        } else {
            int timestamp;
            try {
                timestamp = Integer.valueOf(position);
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Unable to parse position: " + position);
            }
            cursor = logClient.getCursorAtTimestamp(logProject, logStore, shardId, timestamp);
        }
        LOG.info("The starting cursor is {}", cursor);
        return cursor;
    }

    private String restoreCursorFromStateOrCheckpoint(LogstoreShardState state,
                                                      int shardId) throws Exception {
        String cursor = state.getOffset();
        if (cursor != null) {
            LOG.info("Start from restored cursor: {}, shard: {}", cursor, shardId);
            return cursor;
        }
        cursor = findInitialCursor(initialPosition, shardId);
        if (cursor == null) {
            throw new RuntimeException("Unable to find the initial cursor: " + initialPosition);
        }
        return cursor;
    }

    public void run() {
        try {
            LogstoreShardState state = fetcher.getShardState(subscribedShardStateIndex);
            final LogstoreShardMeta shardMeta = state.getShardMeta();
            final int shardId = shardMeta.getShardId();
            LOG.info("Starting shard consumer for shard {}", shardId);
            String cursor = restoreCursorFromStateOrCheckpoint(state, shardId);
            while (isRunning()) {
                PullLogsResponse response = null;
                long fetchStartTimeMs = System.currentTimeMillis();
                try {
                    response = logClient.pullLogs(logProject, logStore, shardId, cursor, fetchSize);
                } catch (LogException ex) {
                    LOG.warn("Failed to pull logs, message: {}, shard: {}", ex.GetErrorMessage(), shardId);
                    // TODO Remove the following code
                    if ("InvalidCursor".equalsIgnoreCase(ex.GetErrorCode())
                            && Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(initialPosition)) {
                        LOG.info("Got invalid cursor error, switch to default position {}", defaultPosition);
                        cursor = findInitialCursor(defaultPosition, shardId);
                    } else {
                        throw ex;
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Fetch request cost {} ms", System.currentTimeMillis() - fetchStartTimeMs);
                }
                if (response != null) {
                    long processingTimeMs;
                    String nextCursor = response.getNextCursor();
                    if (response.getCount() > 0) {
                        long processingStartTimeMs = System.currentTimeMillis();
                        processRecordsAndMoveToNextCursor(response.getLogGroups(), shardId, nextCursor);
                        processingTimeMs = System.currentTimeMillis() - processingStartTimeMs;
                        LOG.debug("Process records cost {} ms.", processingTimeMs);
                        cursor = nextCursor;
                    } else {
                        processingTimeMs = 0;
                        LOG.debug("No records has been responded.");
                        if (cursor.equalsIgnoreCase(nextCursor) && readOnly) {
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
            sleepTime = 500;
        } else if (responseSize < FORCE_SLEEP_THRESHOLD) {
            sleepTime = 200;
        }
        if (sleepTime < fetchIntervalMs) {
            sleepTime = fetchIntervalMs;
        }
        sleepTime -= processingTimeMs;
        if (sleepTime > 0) {
            LOG.debug("Wait {} ms before next fetch", sleepTime);
            Thread.sleep(sleepTime);
        }
    }

    void setReadOnly() {
        this.readOnly = true;
    }

    private void processRecordsAndMoveToNextCursor(List<LogGroupData> records, int shardId, String nextCursor) {
        final T value = deserializer.deserialize(records);
        long timestamp = System.currentTimeMillis();
        if (records.size() > 0) {
            if (records.get(0).GetFastLogGroup().getLogsCount() > 0) {
                long logTimeStamp = records.get(0).GetFastLogGroup().getLogs(0).getTime();
                timestamp = logTimeStamp * 1000;
            }
        }
        fetcher.emitRecordAndUpdateState(
                value,
                timestamp,
                subscribedShardStateIndex,
                nextCursor);
        if (committer != null) {
            committer.updateCheckpoint(shardId, nextCursor);
        }
    }

    private boolean isRunning() {
        return !Thread.interrupted();
    }

    private static int getNumberPerFetch(Properties properties) {
        return PropertiesUtil.getInt(properties, ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, Consts.DEFAULT_NUMBER_PER_FETCH);
    }

    private static long getFetchIntervalMillis(Properties properties) {
        return PropertiesUtil.getLong(properties, ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, Consts.DEFAULT_FETCH_INTERVAL_MILLIS);
    }

    private static String getDefaultPosition(Properties properties) {
        final String val = properties.getProperty(ConfigConstants.LOG_CONSUMER_DEFAULT_POSITION);
        return val != null && !val.isEmpty() ? val : Consts.LOG_BEGIN_CURSOR;
    }
}
