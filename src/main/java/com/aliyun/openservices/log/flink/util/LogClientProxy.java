package com.aliyun.openservices.log.flink.util;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.model.MemoryLimiter;
import com.aliyun.openservices.log.flink.model.ResultHandler;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListLogStoresResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import com.aliyun.openservices.log.util.LZ4Encoder;
import com.aliyun.openservices.log.util.VarintUtil;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class LogClientProxy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LogClientProxy.class);
    private static final long serialVersionUID = -8094827334076355612L;

    private final Client client;
    private final RequestExecutor executor;
    private final MemoryLimiter memoryLimiter;
    private final Lock lock = new ReentrantLock();

    public LogClientProxy(String endpoint,
                          String accessKeyId,
                          String accessKey,
                          String userAgent,
                          RetryPolicy retryPolicy,
                          MemoryLimiter memoryLimiter,
                          ClientConfiguration clientConfiguration) {
        this.client = new Client(endpoint, accessKeyId, accessKey, clientConfiguration);
        this.client.setUserAgent(userAgent);
        this.executor = new RequestExecutor(retryPolicy);
        this.memoryLimiter = memoryLimiter;
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

    public static class PullResult implements Serializable {
        private int count;
        private int rawSize;
        private String nextCursor;

        public PullResult(int count, int rawSize, String nextCursor) {
            this.count = count;
            this.rawSize = rawSize;
            this.nextCursor = nextCursor;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int getRawSize() {
            return rawSize;
        }

        public void setRawSize(int rawSize) {
            this.rawSize = rawSize;
        }

        public String getNextCursor() {
            return nextCursor;
        }

        public void setNextCursor(String nextCursor) {
            this.nextCursor = nextCursor;
        }
    }

    private void parseFastLogGroupList(byte[] uncompressedData,
                                       String requestId,
                                       List<LogGroupData> result,
                                       List<Integer> sizeList) throws LogException {
        int pos = 0;
        int rawSize = uncompressedData.length;
        int mode, index;
        while (pos < rawSize) {
            int[] value = VarintUtil.DecodeVarInt32(uncompressedData, pos, rawSize);
            if (value[0] == 0) {
                throw new LogException("InitLogGroupsError", "decode varint32 error", requestId);
            }
            pos = value[2];
            mode = value[1] & 0x7;
            index = value[1] >> 3;
            if (mode == 0) {
                value = VarintUtil.DecodeVarInt32(uncompressedData, pos, rawSize);
                if (value[0] == 0) {
                    throw new LogException("InitLogGroupsError", "decode varint32 error", requestId);
                }
                pos = value[2];
            } else if (mode == 1) {
                pos += 8;
            } else if (mode == 2) {
                value = VarintUtil.DecodeVarInt32(uncompressedData, pos, rawSize);
                if (value[0] == 0) {
                    throw new LogException("InitLogGroupsError", "decode varint32 error", requestId);
                }
                if (index == 1) {
                    result.add(new LogGroupData(uncompressedData, value[2], value[1], requestId));
                    sizeList.add(value[1]);
                }
                pos = value[1] + value[2];
            } else if (mode == 5) {
                pos += 4;
            } else {
                throw new LogException("InitLogGroupsError", "mode: " + mode, requestId);
            }
        }
        if (pos != rawSize) {
            throw new LogException("InitLogGroupsError", "parse LogGroupList fail", requestId);
        }
    }

    public <T> PullResult pullLogs(String project, String logstore,
                                   int shard, String cursor, String stopCursor, int count,
                                   ResultHandler<T> resultHandler)
            throws LogException, InterruptedException {
        final PullLogsRequest request = new PullLogsRequest(project, logstore, shard, count, cursor, stopCursor);
        if (!memoryLimiter.isEnabled()) {
            PullLogsResponse response = executor.call(() -> client.pullLogs(request), "pullLogs [" + logstore + "] shard=[" + shard + "] ");
            resultHandler.handle(response.getLogGroups(), cursor, response.getNextCursor(), response.getRawSize());
            return new PullResult(response.getCount(), response.getRawSize(), response.getNextCursor());
        }
        lock.lockInterruptibly();
        try {
            PullLogsResponse response = executor.call(() -> client.pullLogs(request), "pullLogs [" + logstore + "] shard=[" + shard + "] ");
            byte[] rawData = response.getRawData();
            List<LogGroupData> logGroups = new ArrayList<>();
            List<Integer> sizeList = new ArrayList<>();
            int rawSize = response.getRawSize();
            if (rawSize > 0) {
                byte[] uncompressedData = LZ4Encoder.decompressFromLhLz4Chunk(rawData, rawSize);
                parseFastLogGroupList(uncompressedData, response.GetRequestId(), logGroups, sizeList);
            }
            if (logGroups.size() != response.getCount()) {
                throw new LogException("LogGroupCountNotMatch", "LogGroup count does match with the count in header message", response.GetRequestId());
            }
            String nextCursor = response.getNextCursor();
            List<LogGroupData> buffer = new ArrayList<>();
            int totalSize = 0;
            for (int i = 0; i < sizeList.size(); ++i) {
                int size = sizeList.get(i);
                if (memoryLimiter.tryAcquire(size)) {
                    totalSize += size;
                    buffer.add(logGroups.get(i));
                    continue;
                } else if (totalSize > 0) {
                    // TODO Fix next cursor
                    resultHandler.handle(buffer, cursor, nextCursor, totalSize);
                    totalSize = 0;
                    buffer.clear();
                }
                memoryLimiter.acquire(size);
                totalSize += size;
                buffer.add(logGroups.get(i));
            }
            resultHandler.handle(buffer, cursor, nextCursor, totalSize);
            return new PullResult(response.getCount(), rawSize, nextCursor);
        } finally {
            lock.unlock();
        }
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

    public void putLogs(String project,
                        String logstore,
                        String topic,
                        String source,
                        String hashKey,
                        List<TagContent> tags,
                        List<LogItem> logItems) throws LogException {
        PutLogsRequest request = new PutLogsRequest(project, logstore, topic,
                source, logItems, hashKey);
        request.SetCompressType(Consts.CompressType.LZ4);
        if (tags != null) {
            request.SetTags(tags);
        }
        executor.call((Callable<Void>) () -> {
            client.PutLogs(request);
            return null;
        }, "PutLogs");
    }

    public void close() {
        if (client != null) {
            executor.cancel();
        }
    }
}
