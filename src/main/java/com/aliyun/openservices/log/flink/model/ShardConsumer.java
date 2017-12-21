package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ShardConsumer<T> implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);
    private final LogDataFetcher<T> fetcherRef;
    private final LogDeserializationSchema<T> deserializer;
    private final int subscribedShardStateIndex;
    private int maxNumberOfRecordsPerFetch = Consts.DEFAULT_NUMBER_PER_FETCH;
    private long fetchIntervalMillis = Consts.DEFAULT_FETCH_INTERVAL_MILLIS;
    private final LogClientProxy logClient;
    private String lastConsumerCursor;
    private final String logProject;
    private final String logStore;
    private String consumerStartPosition;
    private final String consumerGroupName;

    public ShardConsumer(LogDataFetcher<T> fetcher, LogDeserializationSchema<T> deserializer, int subscribedShardStateIndex, Properties configProps, LogClientProxy logClient){
        this.fetcherRef = fetcher;
        this.deserializer = deserializer;
        this.subscribedShardStateIndex = subscribedShardStateIndex;
        this.maxNumberOfRecordsPerFetch = Integer.valueOf(configProps.getProperty(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, Integer.toString(Consts.DEFAULT_NUMBER_PER_FETCH)));
        this.fetchIntervalMillis = Long.valueOf(configProps.getProperty(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, Long.toString(Consts.DEFAULT_FETCH_INTERVAL_MILLIS)));
        this.logClient = logClient;
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.consumerStartPosition = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.consumerGroupName = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
    }

    public void run() {
        try {
            LogstoreShardState state = fetcherRef.getShardState(subscribedShardStateIndex);
            if(state.getShardMeta().getShardStatus().compareToIgnoreCase(Consts.READONLY_SHARD_STATUS) == 0 && state.getShardMeta().getEndCursor() == null){
                String endCursor = logClient.getCursor(logProject, logStore, state.getShardMeta().getShardId(), Consts.LOG_END_CURSOR, "");
                state.getShardMeta().setEndCursor(endCursor);
            }
            lastConsumerCursor = state.getLastConsumerCursor();
            if(lastConsumerCursor == null){
                lastConsumerCursor = logClient.getCursor(logProject, logStore, state.getShardMeta().getShardId(), consumerStartPosition, consumerGroupName);
            }
            while(isRunning()){
                if(state.hasMoreData()){
                    BatchGetLogResponse getLogResponse = null;
                    try {
                        getLogResponse = logClient.getLogs(logProject, logStore, state.getShardMeta().getShardId(), lastConsumerCursor, maxNumberOfRecordsPerFetch);
                    }
                    catch(LogException ex){
                        LOG.warn("getLogs exception, errorcode: {}, errormessage: {}, project : {}, logstore: {}, shard: {}",
                                ex.GetErrorCode(), ex.GetErrorMessage(), logProject, logStore, state.getShardMeta().getShardId());
                        if(ex.GetErrorCode().compareToIgnoreCase("InvalidCursor") == 0){
                            if(consumerStartPosition.compareTo(Consts.LOG_FROM_CHECKPOINT) == 0){
                                consumerStartPosition = Consts.LOG_BEGIN_CURSOR;
                            }
                            lastConsumerCursor = logClient.getCursor(logProject, logStore, state.getShardMeta().getShardId(), consumerStartPosition, consumerGroupName);
                        }
                        else{
                            throw ex;
                        }
                    }

                    if(getLogResponse != null){
                        if(getLogResponse.GetCount() > 0 ) {
                            deserializeRecordForCollectionAndUpdateState(getLogResponse.GetLogGroups(), getLogResponse.GetNextCursor());
                        }
                        long sleepTime = 0;
                        if(getLogResponse.GetRawSize() < 1024 * 1024 && getLogResponse.GetCount() < 100) {
                            sleepTime = 500;
                        }
                        else if(getLogResponse.GetRawSize() < 2 * 1024 * 1024 && getLogResponse.GetCount() < 500) {
                            sleepTime = 200;
                        }
                        else if(getLogResponse.GetRawSize() < 4 * 1024 * 1024 && getLogResponse.GetCount() < 1000) {
                            sleepTime = 50;
                        }
                        if(sleepTime < fetchIntervalMillis)
                            sleepTime = fetchIntervalMillis;
                        Thread.sleep(sleepTime);
                        getLogResponse = null;
                    }
                }
                else{
                    LOG.info("ShardConsumer exit, shard: {}", state.toString());
                    break;
                }
            }
        } catch (Throwable t) {
            LOG.error("unexpected error: {}", t.toString());
            fetcherRef.stopWithError(t);
        }
    }
    private void deserializeRecordForCollectionAndUpdateState(List<LogGroupData> records, String nextCursor)
            throws IOException {
        final T value = deserializer.deserialize(records);
        long timestamp = System.currentTimeMillis();
        if(records.size() > 0){
            if(records.get(0).GetFastLogGroup().getLogsCount() > 0) {
                timestamp = records.get(0).GetFastLogGroup().getLogs(0).getTime() * 1000;
            }
        }
        fetcherRef.emitRecordAndUpdateState(
                value,
                timestamp,
                subscribedShardStateIndex,
                nextCursor);
        lastConsumerCursor = nextCursor;
        records = null;
    }
    private boolean isRunning() {
        return !Thread.interrupted();
    }
}
