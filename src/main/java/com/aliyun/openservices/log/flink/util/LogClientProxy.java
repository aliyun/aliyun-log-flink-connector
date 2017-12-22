package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogClientProxy implements Serializable{
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static int maxRetryTimes = 10;
    private Client logClient;
    public LogClientProxy(String endpoint, String accessKeyId, String accessKey){
        this.logClient = new Client(endpoint, accessKeyId, accessKey);
        this.logClient.setUserAgent("flink-log-connector-0.1.6");
    }
    public String getCursor(String project, String logstore, int shard, String position, String consumerGroup) throws LogException {
        String cursor = null;
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try {
                if (position.compareTo(Consts.LOG_BEGIN_CURSOR) == 0) {
                    cursor = logClient.GetCursor(project, logstore, shard, com.aliyun.openservices.log.common.Consts.CursorMode.BEGIN).GetCursor();
                }
                else if(position.compareTo(Consts.LOG_END_CURSOR) == 0){
                    cursor = logClient.GetCursor(project, logstore, shard, com.aliyun.openservices.log.common.Consts.CursorMode.END).GetCursor();
                }
                else if(position.compareTo(Consts.LOG_FROM_CHECKPOINT) == 0){
                    try {
                        ArrayList<ConsumerGroupShardCheckPoint> cps = logClient.GetCheckPoint(project, logstore, consumerGroup, shard).GetCheckPoints();
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("get checkpoints, p: {}, l: {}, s: {}, position: {}, cg: {}, result: {}",
                                    project,
                                    logstore,
                                    shard,
                                    position,
                                    consumerGroup,
                                    cps.size());
                        }
                        if (cps.size() > 0)
                            cursor = cps.get(0).getCheckPoint();
                        else {
                            position = Consts.LOG_BEGIN_CURSOR;
                            continue;
                        }
                    }
                    catch(LogException ex){
                        LOG.warn("get cursor error, project: {}, logstore: {}, shard: {}, position: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, shard, position, ex.GetErrorCode(), ex.GetErrorMessage(), ex.GetRequestId());
                        if(ex.GetErrorCode().contains("NotExist")){
                            position = Consts.LOG_BEGIN_CURSOR;
                            continue;
                        }
                        else throw ex;
                    }
                }
                else {
                    int time = Integer.valueOf(position);
                    cursor = logClient.GetCursor(project, logstore, shard, time).GetCursor();
                }
                break;
            } catch (LogException e) {
                LOG.warn("get cursor error, project: {}, logstore: {}, shard: {}, position: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, shard, position, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
                if (e.GetErrorCode().contains("Unauthorized") || e.GetErrorCode().contains("NotExist") || e.GetErrorCode().contains("Invalid")) {
                    throw e;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        if(retryTimes >= maxRetryTimes){
            throw new LogException("ExceedMaxRetryTimes", "fail to getCursor", "");
        }
        return cursor;
    }
    public BatchGetLogResponse getLogs(String project, String logstore, int shard, String cursor, int count) throws LogException {
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try {
                return logClient.BatchGetLog(project, logstore, shard, count, cursor);
            } catch (LogException e) {
                LOG.warn("getLogs error, project: {}, logstore: {}, shard: {}, cursor: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, shard, cursor, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
                if (e.GetErrorCode().compareToIgnoreCase("Unauthorized") == 0 || e.GetErrorCode().contains("NotExist") || e.GetErrorCode().contains("Invalid")) {
                    throw e;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        if(retryTimes >= maxRetryTimes){
            throw new LogException("ExceedMaxRetryTimes", "fail to getLogs", "");
        }
        return null;
    }
    public List<LogstoreShardMeta> listShards(String project, String logstore) throws LogException {
        List<LogstoreShardMeta> shards = new ArrayList<LogstoreShardMeta>();
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try{
                for(Shard shard: logClient.ListShard(project, logstore).GetShards()){
                    LogstoreShardMeta shardMeta = new LogstoreShardMeta(shard.GetShardId(), shard.getInclusiveBeginKey(), shard.getExclusiveEndKey(), shard.getStatus());
                    shards.add(shardMeta);
                }
                break;
            }
            catch(LogException e){
                LOG.warn("listShards error, project: {}, logstore: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
                if (e.GetErrorCode().compareToIgnoreCase("Unauthorized") == 0 || e.GetErrorCode().contains("NotExist") || e.GetErrorCode().contains("Invalid")) {
                    throw e;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        if(retryTimes >= maxRetryTimes){
            throw new LogException("ExceedMaxRetryTimes", "fail to listShards", "");
        }
        return shards;
    }

    public void createConsumerGroup(String project, String logstore, String consumerGroup) throws LogException {
        try {
            logClient.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, 100, false));
        } catch (LogException e) {
            LOG.warn("updateCheckpoint error, project: {}, logstore: {}, consumerGroup: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, consumerGroup, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
            if(!e.GetErrorCode().contains("AlreadyExist")) throw e;
        }
    }

    public void updateCheckpoint(String project, String logstore, String consumerGroup, String consumer, int shard, String checkpoint){
        try {
            if(checkpoint != null) {
                logClient.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
            }
        } catch (LogException e) {
            LOG.warn("updateCheckpoint error, project: {}, logstore: {}, consumerGroup: {}, consumer: {}, shard: {}, checkpoint: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, consumerGroup, consumer, shard, checkpoint, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
        }
    }
}
