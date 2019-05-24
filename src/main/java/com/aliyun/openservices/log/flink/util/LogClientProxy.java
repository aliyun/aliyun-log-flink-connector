package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogClientProxy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static final long serialVersionUID = -8094827334076355612L;

    private static int maxRetryTimes = 10;
    private Client logClient;

    public LogClientProxy(String endpoint, String accessKeyId, String accessKey, String userAgent) {
        this.logClient = new Client(endpoint, accessKeyId, accessKey);
        this.logClient.setUserAgent(userAgent);
    }

    public String getCursor(String project, String logstore, int shard, String position, String consumerGroup) throws LogException {
        return getCursor(project, logstore, shard, position, Consts.LOG_BEGIN_CURSOR, consumerGroup);
    }

    public String getCursor(String project, String logstore, int shard, String position, String defaultPosition, String consumerGroup) throws LogException {
        String cursor = null;
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try {
                if (Consts.LOG_BEGIN_CURSOR.equals(position)) {
                    cursor = logClient.GetCursor(project, logstore, shard, CursorMode.BEGIN).GetCursor();
                } else if (Consts.LOG_END_CURSOR.equals(position)) {
                    cursor = logClient.GetCursor(project, logstore, shard, CursorMode.END).GetCursor();
                } else if (Consts.LOG_FROM_CHECKPOINT.equals(position)) {
                    cursor = getConsumerGroupCheckpoint(project, logstore, consumerGroup, shard);
                    if (cursor == null || cursor.isEmpty()) {
                        LOG.info("No available checkpoint for shard {} in consumer group {}, setting to default position {}", shard, consumerGroup, defaultPosition);
                        position = defaultPosition;
                        continue;
                    }
                } else {
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
                Thread.currentThread().interrupt();
            }
            cursor = null;
        }
        if (retryTimes >= maxRetryTimes) {
            throw new LogException("ExceedMaxRetryTimes", "fail to getCursor", "");
        }
        return cursor;
    }

    private String getConsumerGroupCheckpoint(final String project,
                                              final String logstore,
                                              final String consumerGroup,
                                              final int shard) throws LogException {
        try {
            ConsumerGroupCheckPointResponse response = logClient.GetCheckPoint(project, logstore, consumerGroup, shard);
            ArrayList<ConsumerGroupShardCheckPoint> checkpoints = response.GetCheckPoints();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Get checkpoints, project: {}, logstore: {}, shard: {}, consumerGroup: {}, result: {}",
                        project, logstore, shard, consumerGroup, checkpoints != null ? checkpoints.size() : null);
            }
            if (checkpoints == null || checkpoints.isEmpty()) {
                LOG.info("No checkpoint found for shard {}, consumer group {}", shard, consumerGroup);
                return null;
            }
            ConsumerGroupShardCheckPoint checkpoint = checkpoints.get(0);
            if (checkpoint != null) {
                LOG.info("Got checkpoint {} from consumer group {} for shard {}", checkpoint.getCheckPoint(), consumerGroup, shard);
                return checkpoint.getCheckPoint();
            }
        } catch (LogException ex) {
            LOG.warn("Got cursor error, project: {}, logstore: {}, shard: {}, errorcode: {}, errormessage: {}, requestid: {}",
                    project, logstore, shard, ex.GetErrorCode(), ex.GetErrorMessage(), ex.GetRequestId());
            if (!ex.GetErrorCode().contains("NotExist")) {
                throw ex;
            }
        }
        return null;
    }

    public BatchGetLogResponse getLogs(String project, String logstore, int shard, String cursor, int count) throws LogException {
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try {
                return logClient.BatchGetLog(project, logstore, shard, count, cursor);
            } catch (LogException ex) {
                final String errorCode = ex.GetErrorCode();
                LOG.warn("getLogs error, project: {}, logstore: {}, shard: {}, cursor: {}, errorCode: {}, errorMessage: {}, requestId: {}",
                        project, logstore, shard, cursor, errorCode, ex.GetErrorMessage(), ex.GetRequestId());
                if ("Unauthorized".equalsIgnoreCase(errorCode) || errorCode.contains("NotExist") || errorCode.contains("Invalid")) {
                    throw ex;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (retryTimes >= maxRetryTimes) {
            throw new LogException("ExceedMaxRetryTimes", "fail to getLogs", "");
        }
        return null;
    }

    public List<LogstoreShardMeta> listShards(String project, String logstore) throws LogException {
        List<LogstoreShardMeta> shards = new ArrayList<LogstoreShardMeta>();
        int retryTimes = 0;
        while (retryTimes++ < maxRetryTimes) {
            try {
                for (Shard shard : logClient.ListShard(project, logstore).GetShards()) {
                    LogstoreShardMeta shardMeta = new LogstoreShardMeta(shard.GetShardId(), shard.getInclusiveBeginKey(), shard.getExclusiveEndKey(), shard.getStatus());
                    shards.add(shardMeta);
                }
                break;
            } catch (LogException e) {
                final String errorCode = e.GetErrorCode();
                LOG.warn("listShards error, project: {}, logstore: {}, errorCode: {}, errorMessage: {}, requestId: {}",
                        project, logstore, errorCode, e.GetErrorMessage(), e.GetRequestId());
                if ("Unauthorized".equalsIgnoreCase(errorCode) || errorCode.contains("NotExist") || errorCode.contains("Invalid")) {
                    throw e;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (retryTimes >= maxRetryTimes) {
            throw new LogException("ExceedMaxRetryTimes", "fail to listShards", "");
        }
        return shards;
    }

    public void createConsumerGroup(String project, String logstore, String consumerGroup) throws LogException {
        try {
            logClient.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, 100, false));
        } catch (LogException e) {
            LOG.warn("createConsumerGroup error, project: {}, logstore: {}, consumerGroup: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, consumerGroup, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
            if (!e.GetErrorCode().contains("AlreadyExist")) {
                throw e;
            }
        }
    }

    public void updateCheckpoint(String project, String logstore, String consumerGroup, String consumer, int shard, String checkpoint) {
        try {
            if (checkpoint != null) {
                logClient.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
            }
        } catch (LogException e) {
            LOG.warn("updateCheckpoint error, project: {}, logstore: {}, consumerGroup: {}, consumer: {}, shard: {}, checkpoint: {}, errorcode: {}, errormessage: {}, requestid: {}", project, logstore, consumerGroup, consumer, shard, checkpoint, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
        }
    }
}
