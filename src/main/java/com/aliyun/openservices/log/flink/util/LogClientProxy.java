package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

public class LogClientProxy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static final long serialVersionUID = -8094827334076355612L;

    private final Client client;

    public LogClientProxy(String endpoint, String accessKeyId, String accessKey, String userAgent) {
        this.client = new Client(endpoint, accessKeyId, accessKey);
        this.client.setUserAgent(userAgent);
    }

    public String getEndCursor(final String project, final String logstore, final int shard) throws LogException {
        return RetryUtil.call(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return client.GetCursor(project, logstore, shard, CursorMode.END).GetCursor();
            }
        }, "Error while getting end cursor");
    }

    public String getBeginCursor(final String project, final String logstore, final int shard) throws LogException {
        return RetryUtil.call(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return client.GetCursor(project, logstore, shard, CursorMode.BEGIN).GetCursor();
            }
        }, "Error while getting begin cursor");
    }

    public String getCursorAtTimestamp(final String project, final String logstore, final int shard, final int ts) throws LogException {
        return RetryUtil.call(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return client.GetCursor(project, logstore, shard, ts).GetCursor();
            }
        }, "Error while getting cursor with timestamp");
    }

    public String fetchCheckpoint(final String project,
                                  final String logstore,
                                  final String consumerGroup,
                                  final int shard) throws LogException {
        return RetryUtil.call(new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    ConsumerGroupCheckPointResponse response = client.GetCheckPoint(project, logstore, consumerGroup, shard);
                    List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
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
                    if (!ex.GetErrorCode().contains("NotExist")) {
                        throw ex;
                    }
                }
                return null;
            }
        }, "Error while getting checkpoint");
    }

    public PullLogsResponse pullLogs(String project, String logstore, int shard, String cursor, int count)
            throws LogException {
        final PullLogsRequest request = new PullLogsRequest(project, logstore, shard, count, cursor);
        return RetryUtil.call(new Callable<PullLogsResponse>() {
            @Override
            public PullLogsResponse call() throws Exception {
                return client.pullLogs(request);
            }
        }, "Error while pulling logs");
    }

    public List<Shard> listShards(final String project, final String logstore) throws LogException {
        return RetryUtil.call(new Callable<List<Shard>>() {
            @Override
            public List<Shard> call() throws Exception {
                return client.ListShard(project, logstore).GetShards();
            }
        }, "Error while listing shards");
    }

    public void createConsumerGroup(final String project, final String logstore, final String consumerGroupName)
            throws Exception {
        RetryUtil.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ConsumerGroup consumerGroup = new ConsumerGroup(consumerGroupName, 100, false);
                try {
                    client.CreateConsumerGroup(project, logstore, consumerGroup);
                } catch (LogException ex) {
                    if ("ConsumerGroupAlreadyExist".equals(ex.GetErrorCode())) {
                        return null;
                    }
                    throw ex;
                }
                return null;
            }
        }, "Error while creating consumer group");
    }

    public void updateCheckpoint(final String project,
                                 final String logstore,
                                 final String consumerGroup,
                                 final int shard,
                                 final boolean readOnly,
                                 final String checkpoint) throws LogException {
        if (checkpoint == null || checkpoint.isEmpty()) {
            LOG.warn("The checkpoint to update is invalid: {}", checkpoint);
            return;
        }
        try {
            RetryUtil.call(new Callable<Void>() {
                @Override
                public Void call() throws LogException {
                    client.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
                    return null;
                }
            }, "Error while updating checkpoint");
        } catch (LogException ex) {
            if ("ShardNotExist".equalsIgnoreCase(ex.GetErrorCode())) {
                LOG.warn("Shard {} not exist, readonly = {}", shard, readOnly);
            } else {
                throw ex;
            }
        }
    }
}
