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
    private static final long FORCE_SLEEP_MS = 500;

    private final LogDataFetcher<T> fetcher;
    private final LogDeserializationSchema<T> deserializer;
    private final int subscribedShardStateIndex;
    private int maxNumberOfRecordsPerFetch;
    private long fetchIntervalMillis;
    private final LogClientProxy logClient;
    private String currentCursor;
    private final String logProject;
    private final String logStore;
    private String consumerStartPosition;
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
        this.maxNumberOfRecordsPerFetch = getNumberPerFetch(configProps);
        this.fetchIntervalMillis = getFetchIntervalMillis(configProps);
        this.logClient = logClient;
        this.committer = committer;
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.consumerStartPosition = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(consumerStartPosition)
                && (consumerGroup == null || consumerGroup.isEmpty())) {
            throw new IllegalArgumentException("Missing parameter: " + ConfigConstants.LOG_CONSUMERGROUP);
        }
        defaultPosition = getDefaultPosition(configProps);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(defaultPosition)) {
            throw new IllegalArgumentException("Cannot use " + Consts.LOG_FROM_CHECKPOINT + " as the default position");
        }
    }

    public void run() {
        try {
            LogstoreShardState state = fetcher.getShardState(subscribedShardStateIndex);
            final LogstoreShardMeta shardMeta = state.getShardMeta();
            final int shardId = shardMeta.getShardId();
            LOG.info("Starting shard consumer for shard {}", shardId);
            if (shardMeta.needSetEndCursor()) {
                String endCursor = logClient.getEndCursor(logProject, logStore, shardId);
                LOG.info("The latest cursor of shard {} is {}", shardId, endCursor);
                shardMeta.setEndCursor(endCursor);
            }
            currentCursor = state.getOffset();
            if (currentCursor == null) {
                currentCursor = logClient.getCursor(logProject, logStore, shardId, consumerStartPosition, defaultPosition, consumerGroup);
                LOG.info("init cursor success, p: {}, l: {}, s: {}, cursor: {}", logProject, logStore, shardId, currentCursor);
            }
            long elapsedTime;
            while (isRunning()) {
                PullLogsResponse response = null;
                elapsedTime = System.currentTimeMillis();
                try {
                    response = logClient.pullLogs(logProject, logStore, shardId, currentCursor, maxNumberOfRecordsPerFetch);
                } catch (LogException ex) {
                    LOG.warn("getLogs exception, errorcode: {}, errormessage: {}, project : {}, logstore: {}, shard: {}",
                            ex.GetErrorCode(), ex.GetErrorMessage(), logProject, logStore, shardId);
                    if ("InvalidCursor".equalsIgnoreCase(ex.GetErrorCode())) {
                        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(consumerStartPosition)) {
                            LOG.info("Got invalid cursor error, switch to default position {}", defaultPosition);
                            consumerStartPosition = defaultPosition;
                        }
                        currentCursor = logClient.getCursor(logProject, logStore, shardId, consumerStartPosition, consumerGroup);
                    } else {
                        throw ex;
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Pull log request cost {}", System.currentTimeMillis() - elapsedTime);
                }
                if (response != null) {
                    elapsedTime = System.currentTimeMillis();
                    String nextCursor = response.getNextCursor();
                    if (response.getCount() > 0) {
                        processRecordsAndMoveToNextCursor(response.getLogGroups(), shardId, nextCursor);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Process records cost {}", System.currentTimeMillis() - elapsedTime);
                    }
                    if (currentCursor.equalsIgnoreCase(nextCursor) && readOnly) {
                        LOG.info("Shard {} is finished", shardId);
                        break;
                    }
                    long sleepTime = 0;
                    int size = response.getRawSize();
                    if (size < FORCE_SLEEP_THRESHOLD) {
                        sleepTime = FORCE_SLEEP_MS;
                    }
                    if (sleepTime < fetchIntervalMillis)
                        sleepTime = fetchIntervalMillis;
                    if (sleepTime > 0) {
                        LOG.debug("Wait {} ms before next pull request", sleepTime);
                        Thread.sleep(sleepTime);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("Unexpected error", t);
            fetcher.stopWithError(t);
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
            committer.updateCheckpoint(shardId, currentCursor);
        }
        currentCursor = nextCursor;
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
