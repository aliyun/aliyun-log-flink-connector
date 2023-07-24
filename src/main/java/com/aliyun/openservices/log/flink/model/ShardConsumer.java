package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ShardConsumer<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    private static final int FORCE_SLEEP_THRESHOLD = 256 * 1024;
    private static final String ERR_SHARD_NOT_EXIST = "ShardNotExist";
    private static final String ERR_INVALID_CURSOR = "InvalidCursor";

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
    private volatile boolean isReadOnly = false;
    private volatile boolean isRunning = true;
    private int stopTimeSec = -1;
    private final CompletableFuture<Void> cancelFuture = new CompletableFuture<>();
    private final LogDataFetcher.RecordEmitter<T> recordEmitter;

    ShardConsumer(LogDataFetcher<T> fetcher,
                  LogDeserializationSchema<T> deserializer,
                  int subscribedShardStateIndex,
                  Properties configProps,
                  LogClientProxy logClient,
                  LogDataFetcher.RecordEmitter<T> recordEmitter) {
        this.fetcher = fetcher;
        this.deserializer = deserializer;
        this.subscribedShardStateIndex = subscribedShardStateIndex;
        // TODO Move configs to a class
        this.fetchSize = LogUtil.getNumberPerFetch(configProps);
        this.fetchIntervalMs = LogUtil.getFetchIntervalMillis(configProps);
        this.logClient = logClient;
        this.recordEmitter = recordEmitter;
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
        String stopTime = configProps.getProperty(ConfigConstants.STOP_TIME);
        if (stopTime != null && !stopTime.isEmpty()) {
            stopTimeSec = Integer.parseInt(stopTime);
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
        if (cursor != null && !cursor.isEmpty()) {
            LOG.info("Restored cursor from Flink state: {}, shard: {}", cursor, shardId);
            return cursor;
        }
        cursor = findInitialCursor(logstore, initialPosition, shardId);
        if (cursor == null || cursor.isEmpty()) {
            throw new RuntimeException("Unable to find the initial cursor from: " + initialPosition);
        }
        return cursor;
    }

    private String getStopCursor(String logstore, int shard) {
        if (stopTimeSec > 0) {
            try {
                return logClient.getCursorAtTimestamp(logProject, logstore, shard, stopTimeSec);
            } catch (LogException ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    public void run() {
        try {
            LogstoreShardState state = fetcher.getShardState(subscribedShardStateIndex);
            final LogstoreShardMeta shardMeta = state.getShardMeta();
            final int shardId = shardMeta.getShardId();
            String logstore = shardMeta.getLogstore();
            String query = shardMeta.getQuery();
            String cursor = restoreCursorFromStateOrCheckpoint(logstore, state.getOffset(), shardId);
            String stopCursor = getStopCursor(logstore, shardId);
            LOG.info("Starting consumer for shard {} with initial cursor {}", shardId, cursor);
            ResultHandler<T> resultHandler = new ResultHandler<>(shardMeta, this);
            while (isRunning) {
                LogClientProxy.PullResult response;
                long fetchStartTimeMs = System.currentTimeMillis();
                try {
                    response = logClient.pullLogs(logProject, logstore, shardId, cursor, stopCursor, query, fetchSize, resultHandler);
                } catch (LogException ex) {
                    LOG.warn("Failed to pull logs, message: {}, shard: {}", ex.GetErrorMessage(), shardId);
                    String errorCode = ex.GetErrorCode();
                    if (ERR_SHARD_NOT_EXIST.equalsIgnoreCase(errorCode)) {
                        // The shard has been deleted
                        LOG.warn("The shard {} already not exist, project {} logstore {}", shardId, logProject, logstore);
                        break;
                    }
                    // TODO PullData will never throw InvalidCursor
                    if (ERR_INVALID_CURSOR.equalsIgnoreCase(errorCode)
                            && Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(initialPosition)) {
                        LOG.warn("Cursor is invalid {}, start from default position {}", cursor, defaultPosition);
                        cursor = findInitialCursor(logstore, defaultPosition, shardId);
                        continue;
                    }
                    throw ex;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Fetch request cost {} ms", System.currentTimeMillis() - fetchStartTimeMs);
                }
                if (response == null) {
                    continue;
                }
                if (!isRunning)
                    break;
                String nextCursor = response.getNextCursor();
                if (response.getCount() > 0) {
                    cursor = nextCursor;
                    adjustFetchFrequency(response.getRawSize());
                    continue;
                }
                if ((cursor.equals(nextCursor) && isReadOnly)
                        || cursor.equals(stopCursor)) {
                    LOG.info("Shard [{}] is finished, readonly={}, stopCursor={}", shardMeta.getId(), isReadOnly, stopCursor);
                    break;
                }
                adjustFetchFrequency(response.getRawSize());
            }
            LOG.warn("Consumer for shard {} stopped", shardId);
            fetcher.complete(shardMeta.getId());
        } catch (Throwable t) {
            LOG.error("Unexpected error", t);
            fetcher.stopWithError(t);
        }
    }

    private void adjustFetchFrequency(int responseSize) throws Exception {
        long sleepTime = 0;
        if (responseSize <= 1) {
            // Outflow: 1
            sleepTime = 500;
        } else if (responseSize < FORCE_SLEEP_THRESHOLD) {
            sleepTime = 200;
        }
        if (sleepTime < fetchIntervalMs) {
            sleepTime = fetchIntervalMs;
        }
        if (sleepTime > 0) {
            LOG.debug("Wait {} ms before next fetching", sleepTime);
            try {
                cancelFuture.get(sleepTime, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                // timeout is expected when consumer is not stopped
            }
        }
    }

    void markAsReadOnly() {
        this.isReadOnly = true;
    }

    void cancel() {
        isRunning = false;
        cancelFuture.complete(null);
    }

    public void processRecords(List<LogGroupData> records,
                               String cursor,
                               LogstoreShardMeta shard,
                               String nextCursor,
                               int dataRawSize) throws InterruptedException {
        PullLogsResult record = new PullLogsResult(records, shard.getShardId(), cursor, nextCursor);
        final T value = deserializer.deserialize(record);
        long timestamp = System.currentTimeMillis();
        if (!records.isEmpty()) {
            // Use the timestamp of first log for perf consideration.
            FastLogGroup logGroup = records.get(0).GetFastLogGroup();
            if (logGroup.getLogsCount() > 0) {
                long logTimeStamp = logGroup.getLogs(0).getTime();
                timestamp = logTimeStamp * 1000;
            }
        }
        SourceRecord<T> sourceRecord = new SourceRecord<>(
                value, timestamp, subscribedShardStateIndex, nextCursor, shard, isReadOnly, dataRawSize);
        recordEmitter.produce(sourceRecord);
    }
}
