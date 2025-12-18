package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.MemoryLimiter;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.http.signer.SignVersion;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListLogStoresResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public class LogClientProxy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static final long serialVersionUID = -8094827334076355612L;

    private final Client client;
    private final RequestExecutor executor;
    private final MemoryLimiter memoryLimiter;
    private final String processor;

    public LogClientProxy(String endpoint,
                          String accessKeyId,
                          String accessKey,
                          String userAgent,
                          RetryPolicy retryPolicy,
                          MemoryLimiter memoryLimiter,
                          ClientConfiguration clientConfiguration,
                          String processor) {
        this.client = new Client(endpoint, accessKeyId, accessKey, clientConfiguration);
        this.client.setUserAgent(userAgent);
        this.executor = new RequestExecutor(retryPolicy);
        this.memoryLimiter = memoryLimiter;
        this.processor = processor;
    }

    public static LogClientProxy makeClient(Properties configProps,
                                            String accessKeyId,
                                            String accessKey,
                                            int subtaskIndex) {
        ConfigParser parser = new ConfigParser(configProps);
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(parser.getInt(ConfigConstants.MAX_RETRIES, com.aliyun.openservices.log.flink.util.Consts.DEFAULT_MAX_RETRIES))
                .maxRetriesForRetryableError(parser.getInt(ConfigConstants.MAX_RETRIES_FOR_RETRYABLE_ERROR,
                        com.aliyun.openservices.log.flink.util.Consts.DEFAULT_MAX_RETRIES_FOR_RETRYABLE_ERROR))
                .baseRetryBackoff(parser.getLong(ConfigConstants.BASE_RETRY_BACKOFF_TIME_MS,
                        com.aliyun.openservices.log.flink.util.Consts.DEFAULT_BASE_RETRY_BACKOFF_TIME_MS))
                .maxRetryBackoff(parser.getLong(ConfigConstants.MAX_RETRY_BACKOFF_TIME_MS,
                        com.aliyun.openservices.log.flink.util.Consts.DEFAULT_MAX_RETRY_BACKOFF_TIME_MS))
                .build();

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxConnections(com.aliyun.openservices.log.common.Consts.HTTP_CONNECT_MAX_COUNT);
        clientConfig.setConnectionTimeout(com.aliyun.openservices.log.common.Consts.HTTP_CONNECT_TIME_OUT);
        clientConfig.setSocketTimeout(com.aliyun.openservices.log.common.Consts.HTTP_SEND_TIME_OUT);
        clientConfig.setProxyHost(parser.getString(ConfigConstants.PROXY_HOST));
        clientConfig.setProxyPort(parser.getInt(ConfigConstants.PROXY_PORT, -1));
        clientConfig.setProxyUsername(parser.getString(ConfigConstants.PROXY_USERNAME));
        clientConfig.setProxyPassword(parser.getString(ConfigConstants.PROXY_PASSWORD));
        clientConfig.setProxyDomain(parser.getString(ConfigConstants.PROXY_DOMAIN));
        clientConfig.setProxyWorkstation(parser.getString(ConfigConstants.PROXY_WORKSTATION));

        SignVersion signVersion = LogUtil.parseSignVersion(parser.getString(ConfigConstants.SIGNATURE_VERSION));
        if (signVersion == SignVersion.V4) {
            String regionId = parser.getString(ConfigConstants.REGION_ID);
            if (regionId == null || regionId.isEmpty()) {
                throw new IllegalArgumentException("The " + ConfigConstants.REGION_ID + " was not specified for signature " + signVersion.name() + ".");
            }
            clientConfig.setRegion(regionId);
        }
        clientConfig.setSignatureVersion(signVersion);
        MemoryLimiter memoryLimiter = new MemoryLimiter(configProps);
        String userAgent = resolveUserAgent(configProps, subtaskIndex);
        String processor = parser.getString(ConfigConstants.PROCESSOR);
        return new LogClientProxy(
                parser.getString(ConfigConstants.LOG_ENDPOINT),
                accessKeyId,
                accessKey,
                userAgent,
                retryPolicy,
                memoryLimiter,
                clientConfig,
                processor);
    }

    private static String resolveUserAgent(Properties configProps, int subtaskIndex) {
        String userAgent = configProps.getProperty(ConfigConstants.LOG_USER_AGENT);
        if (userAgent == null || userAgent.isEmpty()) {
            userAgent = "flink-connector/" + com.aliyun.openservices.log.flink.util.Consts.FLINK_CONNECTOR_VERSION
                    + " (Subtask " + subtaskIndex + ")";
        }
        return userAgent;
    }

    public String getEndCursor(final String project, final String logstore, final int shard) throws LogException {
        return executor.call(() -> client.GetCursor(project, logstore, shard, CursorMode.END).GetCursor(), "getEndCursor");
    }

    public String getBeginCursor(final String project, final String logstore, final int shard) throws LogException {
        return executor.call(() -> client.GetCursor(project, logstore, shard, CursorMode.BEGIN).GetCursor(), "getBeginCursor");
    }

    public String getCursorAtTimestamp(final String project, final String logstore, final int shard, final int ts) throws LogException {
        return executor.call(() -> client.GetCursor(project, logstore, shard, ts).GetCursor(), "getCursorAtTimestamp");
    }

    public String fetchCheckpoint(final String project,
                                  final String logstore,
                                  final String consumerGroup,
                                  final int shard) throws LogException {
        return executor.call(() -> {
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
        }, "fetchCheckpoint");
    }

    public PullLogsResult pullLogs(String project,
                                   String logstore,
                                   int shard,
                                   String cursor,
                                   String stopCursor,
                                   int count)
            throws LogException, InterruptedException {
        final PullLogsRequest request = new PullLogsRequest(project, logstore, shard, count, cursor, stopCursor);
        if (processor != null && !processor.isEmpty()) {
            request.setProcessor(processor);
        }
        PullLogsResponse response = executor.call(() -> client.pullLogs(request), "pullLogs [" + logstore + "] shard=[" + shard + "] ");
        int rawSize = response.getRawSize();
        memoryLimiter.acquire(rawSize);
        String readLastCursor = response.GetHeader("x-log-read-last-cursor");
        return new PullLogsResult(response.getLogGroups(),
                shard,
                cursor,
                response.getNextCursor(),
                readLastCursor,
                response.getRawSize(),
                response.getCount(),
                response.getCursorTime() * 1000L);
    }

    @VisibleForTesting
    static void filter(List<String> logstores, Pattern pattern, List<String> result) {
        for (String logstore : logstores) {
            if (pattern == null || pattern.matcher(logstore).matches()) {
                result.add(logstore);
            }
        }
    }

    public List<String> listLogstores(String project, Pattern pattern) {
        List<String> logstores = new ArrayList<>();
        try {
            int offset = 0;
            int batchSize = 100;
            while (true) {
                ListLogStoresResponse response = client.ListLogStores(project, offset, batchSize, "");
                filter(response.GetLogStores(), pattern, logstores);
                if (response.GetCount() < batchSize) {
                    break;
                }
                offset += batchSize;
            }
        } catch (LogException ex) {
            LOG.warn("Error listing logstores", ex);
        }
        return logstores;
    }

    public List<Shard> listShards(final String project, final String logstore) throws LogException {
        return executor.call((Callable<List<Shard>>) () -> client.ListShard(project, logstore).GetShards(), "listShards");
    }

    public boolean checkConsumerGroupExists(String project, String logstore, String consumerGroup) throws Exception {
        ListConsumerGroupResponse response = client.ListConsumerGroup(project, logstore);
        if (response != null) {
            for (ConsumerGroup item : response.GetConsumerGroups()) {
                if (item.getConsumerGroupName().equals(consumerGroup)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createConsumerGroup(final String project,
                                    final String logstore,
                                    final String consumerGroupName) throws LogException {
        executor.call((Callable<Void>) () -> {
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
        }, "createConsumerGroup");
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
            executor.call((Callable<Void>) () -> {
                client.UpdateCheckPoint(project, logstore, consumerGroup, shard, checkpoint);
                return null;
            }, "updateCheckpoint");
        } catch (LogException ex) {
            if ("ConsumerGroupNotExist".equalsIgnoreCase(ex.GetErrorCode())) {
                LOG.warn("Consumer group not exist: {}", consumerGroup);
            } else if ("ShardNotExist".equalsIgnoreCase(ex.GetErrorCode())) {
                LOG.warn("Shard {} not exist, readonly = {}", shard, readOnly);
            } else {
                throw ex;
            }
        }
    }

    public void close() {
        if (client != null) {
            executor.cancel();
        }
    }
}
