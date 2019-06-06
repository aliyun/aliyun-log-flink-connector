package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogClientProxy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static final long serialVersionUID = -8094827334076355612L;

    private static final int MAX_ATTEMPTS = 10;
    private static final long RETRY_INTERVAL_MS = 100;
    private final Client logClient;

    public LogClientProxy(String endpoint, String accessKeyId, String accessKey, String userAgent) {
        this.logClient = new Client(endpoint, accessKeyId, accessKey);
        this.logClient.setUserAgent(userAgent);
    }

    public String getCursor(String project, String logstore, int shard, String position, String consumerGroup) throws LogException {
        return getCursor(project, logstore, shard, position, Consts.LOG_BEGIN_CURSOR, consumerGroup);
    }

    public String getEndCursor(String project, String logstore, int shard) {
        LogException lastException = null;
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            try {
                return logClient.GetCursor(project, logstore, shard, CursorMode.END).GetCursor();
            } catch (LogException lex) {
                lastException = lex;
                LOG.warn("Failed to fetch end cursor, code {}, message {}, requestID {}.",
                        lex.GetErrorCode(), lex.GetErrorMessage(), lex.GetRequestId());
            }
            if (i < MAX_ATTEMPTS - 1) {
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new RuntimeException(lastException);
    }

    public String getCursor(String project, String logstore, int shard,
                            String position, String defaultPosition, String consumerGroup) throws LogException {
        String cursor = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                if (Consts.LOG_BEGIN_CURSOR.equals(position)) {
                    cursor = logClient.GetCursor(project, logstore, shard, CursorMode.BEGIN).GetCursor();
                } else if (Consts.LOG_END_CURSOR.equals(position)) {
                    cursor = logClient.GetCursor(project, logstore, shard, CursorMode.END).GetCursor();
                } else if (Consts.LOG_FROM_CHECKPOINT.equals(position)) {
                    cursor = fetchCheckpoint(project, logstore, consumerGroup, shard);
                    if (cursor == null || cursor.isEmpty()) {
                        LOG.info("No available checkpoint for shard {} in consumer group {}, setting to default position {}",
                                shard, consumerGroup, defaultPosition);
                        position = defaultPosition;
                        continue;
                    }
                } else {
                    int time = Integer.valueOf(position);
                    cursor = logClient.GetCursor(project, logstore, shard, time).GetCursor();
                }
                break;
            } catch (LogException lex) {
                if (isRecoverableException(lex) && attempt < MAX_ATTEMPTS - 1) {
                    LOG.warn("Failed to fetch cursor, project {}, logstore {}, shard {}, position {}," +
                                    "errorCode {}, message {}, request ID {}. attempt {}",
                            project, logstore, shard, position, lex.GetErrorCode(),
                            lex.GetErrorMessage(), lex.GetRequestId(), attempt);
                } else {
                    throw lex;
                }
            }
            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return cursor;
    }

    private static boolean isRecoverableException(LogException lex) {
        final String errorCode = lex.GetErrorCode();
        return !errorCode.contains("Unauthorized") && !errorCode.contains("NotExist") && !errorCode.contains("Invalid");
    }

    private String fetchCheckpoint(final String project,
                                   final String logstore,
                                   final String consumerGroup,
                                   final int shard) throws LogException {
        try {
            ConsumerGroupCheckPointResponse response = logClient.GetCheckPoint(project, logstore, consumerGroup, shard);
            List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
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

    public PullLogsResponse pullLogs(String project, String logstore, int shard, String cursor, int count) throws LogException {
        PullLogsRequest request = new PullLogsRequest(project, logstore, shard, count, cursor);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                return logClient.pullLogs(request);
            } catch (LogException lex) {
                if (isRecoverableException(lex) && attempt < MAX_ATTEMPTS - 1) {
                    LOG.warn("Failed to pull logs from project {}, logstore {}, shard {}, cursor {}" +
                                    "errorCode {}, message {}, request ID {}. attempt {}",
                            project, logstore, shard, cursor, lex.GetErrorCode(),
                            lex.GetErrorMessage(), lex.GetRequestId(), attempt);
                } else {
                    throw lex;
                }
            }
            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    public List<LogstoreShardMeta> listShards(String project, String logstore) throws LogException {
        List<LogstoreShardMeta> shards = new ArrayList<LogstoreShardMeta>();
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                for (Shard shard : logClient.ListShard(project, logstore).GetShards()) {
                    LogstoreShardMeta shardMeta = new LogstoreShardMeta(shard.GetShardId(), shard.getInclusiveBeginKey(), shard.getExclusiveEndKey(), shard.getStatus());
                    shards.add(shardMeta);
                }
                break;
            } catch (LogException lex) {
                if (isRecoverableException(lex) && attempt < MAX_ATTEMPTS - 1) {
                    LOG.warn("Failed to list shards for project {}, logstore {}, " +
                                    "errorCode {}, message {}, request ID {}. attempt {}",
                            project, logstore, lex.GetErrorCode(),
                            lex.GetErrorMessage(), lex.GetRequestId(), attempt);
                } else {
                    throw lex;
                }
            }
            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return shards;
    }

    public void createConsumerGroup(String project, String logstore, String consumerGroup) throws LogException {
        try {
            logClient.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, 100, false));
        } catch (LogException e) {
            LOG.warn("Failed to create consumer group: {}, code: {}," +
                            " message: {}, requestID: {}",
                    consumerGroup, e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
            if (!e.GetErrorCode().contains("AlreadyExist")) {
                throw e;
            }
        }
    }

    public void updateCheckpoint(String project, String logstore, String consumerGroup, int shard, String checkpoint) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            LOG.warn("The checkpoint to update is invalid: {}", checkpoint);
            return;
        }
        try {
            logClient.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
        } catch (LogException e) {
            LOG.error("Failed to update checkpoint error, consumerGroup: {}," +
                            " shard: {}, checkpoint: {}, code: {}, message: {}," +
                            " requestID: {}",
                    consumerGroup, shard, checkpoint,
                    e.GetErrorCode(), e.GetErrorMessage(), e.GetRequestId());
        }
    }
}
