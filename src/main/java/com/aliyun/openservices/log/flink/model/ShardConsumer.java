package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.apache.flink.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ShardConsumer<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);
    private final LogDataFetcher<T> fetcherRef;
    private final LogDeserializationSchema<T> deserializer;
    private final int subscribedShardStateIndex;
    private int maxNumberOfRecordsPerFetch;
    private long fetchIntervalMillis;
    private final LogClientProxy logClient;
    private String lastConsumerCursor;
    private final String logProject;
    private final String logStore;
    private String consumerStartPosition;
    private final String defaultPosition;
    private final String consumerGroupName;
    private volatile boolean readOnly = false;
    private static final int FORCE_SLEEP_THRESHOLD = 512 * 1024;
    private static final long FORCE_SLEEP_MS = 500;

    public ShardConsumer(LogDataFetcher<T> fetcher, LogDeserializationSchema<T> deserializer, int subscribedShardStateIndex, Properties configProps, LogClientProxy logClient) {
        this.fetcherRef = fetcher;
        this.deserializer = deserializer;
        this.subscribedShardStateIndex = subscribedShardStateIndex;
        this.maxNumberOfRecordsPerFetch = getNumberPerFetch(configProps);
        this.fetchIntervalMillis = getFetchIntervalMillis(configProps);
        this.logClient = logClient;
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.consumerStartPosition = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.consumerGroupName = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(consumerStartPosition)
                && (consumerGroupName == null || consumerGroupName.isEmpty())) {
            throw new IllegalArgumentException("The setting " + ConfigConstants.LOG_CONSUMERGROUP + " is required for restoring checkpoint from consumer group");
        }
        defaultPosition = getDefaultPosition(configProps);
        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(defaultPosition)) {
            throw new IllegalArgumentException("Cannot use " + Consts.LOG_FROM_CHECKPOINT + " as the default position");
        }
    }

    @Override
    public void run() {
        try {
            LogstoreShardState state = fetcherRef.getShardState(subscribedShardStateIndex);
            final LogstoreShardMeta shardMeta = state.getShardMeta();
            final int shardId = shardMeta.getShardId();
            if (shardMeta.isReadOnly() && state.getShardMeta().getEndCursor() == null) {
                String endCursor = logClient.getCursor(logProject, logStore, shardId, Consts.LOG_END_CURSOR, "");
                state.getShardMeta().setEndCursor(endCursor);
            }
            lastConsumerCursor = state.getLastConsumerCursor();
            if (lastConsumerCursor == null) {
                lastConsumerCursor = logClient.getCursor(logProject, logStore, shardId, consumerStartPosition, defaultPosition, consumerGroupName);
                LOG.info("init cursor success, p: {}, l: {}, s: {}, cursor: {}", logProject, logStore, shardId, lastConsumerCursor);
            }
            while (isRunning()) {
                if (!state.hasMoreData()) {
                    LOG.info("ShardConsumer exit, shard: {}", state.toString());
                    break;
                }
                BatchGetLogResponse getLogResponse = null;
                try {
                    getLogResponse = logClient.getLogs(logProject, logStore, shardId, lastConsumerCursor, maxNumberOfRecordsPerFetch);
                } catch (LogException ex) {
                    LOG.warn("getLogs exception, errorcode: {}, errormessage: {}, project : {}, logstore: {}, shard: {}",
                            ex.GetErrorCode(), ex.GetErrorMessage(), logProject, logStore, shardId);
                    if ("InvalidCursor".equalsIgnoreCase(ex.GetErrorCode())) {
                        if (Consts.LOG_FROM_CHECKPOINT.equalsIgnoreCase(consumerStartPosition)) {
                            LOG.info("Got invalid cursor error, switch to default position {}", defaultPosition);
                            consumerStartPosition = defaultPosition;
                        }
                        lastConsumerCursor = logClient.getCursor(logProject, logStore, shardId, consumerStartPosition, consumerGroupName);
                    } else {
                        throw ex;
                    }
                }
                if (getLogResponse != null) {
                    String nextCursor = getLogResponse.GetNextCursor();
                    if (getLogResponse.GetCount() > 0) {
                        processRecordsAndMoveToNextCursor(getLogResponse.GetLogGroups(), nextCursor);
                    }
                    if (lastConsumerCursor.equalsIgnoreCase(nextCursor) && readOnly) {
                        LOG.info("Shard {} is finished", shardId);
                        break;
                    }
                    long sleepTime = 0;
                    int size = getLogResponse.GetRawSize();
                    if (size < FORCE_SLEEP_THRESHOLD) {
                        sleepTime = FORCE_SLEEP_MS;
                    }
                    if (sleepTime < fetchIntervalMillis)
                        sleepTime = fetchIntervalMillis;
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("unexpected error", t);
            fetcherRef.stopWithError(t);
        }
    }

    void setReadOnly() {
        this.readOnly = true;
    }

    private void processRecordsAndMoveToNextCursor(List<LogGroupData> records, String nextCursor) {
        final T value = deserializer.deserialize(records);
        long timestamp = System.currentTimeMillis();
        if (records.size() > 0) {
            if (records.get(0).GetFastLogGroup().getLogsCount() > 0) {
                long logTimeStamp = records.get(0).GetFastLogGroup().getLogs(0).getTime();
                timestamp = logTimeStamp * 1000;
            }
        }
        fetcherRef.emitRecordAndUpdateState(
                value,
                timestamp,
                subscribedShardStateIndex,
                nextCursor);
        lastConsumerCursor = nextCursor;
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
