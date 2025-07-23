package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.ShardAssigner;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

public class LogDataFetcher<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogDataFetcher.class);
    public static final ShardAssigner DEFAULT_SHARD_ASSIGNER = new DefaultShardAssigner();
    private static final int DEFAULT_QUEUE_SIZE = 1;
    private static final long DEFAULT_IDLE_INTERVAL = 10;

    private final Properties configProps;
    private final LogDeserializationSchema<T> deserializer;
    private final int totalNumberOfSubtasks;
    private final int indexOfThisSubtask;
    private final SourceFunction.SourceContext<T> sourceContext;
    private final Object checkpointLock;
    private final ExecutorService shardConsumersExecutor;
    private final AtomicInteger numberOfActiveShards = new AtomicInteger(0);
    private final AtomicReference<Throwable> error;
    private final LogClientProxy logClient;
    private volatile boolean running = true;
    private volatile List<LogstoreShardState> subscribedShardsState;
    private final String project;
    private final CheckpointMode checkpointMode;
    private final String consumerGroup;
    private CheckpointCommitter autoCommitter;
    private long commitInterval;
    private Map<String, ShardConsumer<T>> consumerCache;
    private final Pattern logstorePattern;
    private final List<String> logstores;
    private final Set<String> subscribedLogstores;
    private final ShardAssigner shardAssigner;
    private boolean exitAfterAllShardFinished = false;
    private final CompletableFuture<Void> cancelFuture = new CompletableFuture<>();
    private final MemoryLimiter memoryLimiter;
    private final BlockingQueue<SourceRecord<T>> queue;
    private RecordEmitter<T> recordEmitter;

    public LogDataFetcher(SourceFunction.SourceContext<T> sourceContext,
                          RuntimeContext context,
                          String project,
                          List<String> logstores,
                          Pattern logstorePattern,
                          Properties configProps,
                          LogDeserializationSchema<T> deserializer,
                          LogClientProxy logClient,
                          CheckpointMode checkpointMode,
                          ShardAssigner shardAssigner,
                          MemoryLimiter memoryLimiter) {
        this.sourceContext = sourceContext;
        this.configProps = configProps;
        this.deserializer = deserializer;
        this.totalNumberOfSubtasks = context.getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = context.getIndexOfThisSubtask();
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.shardAssigner = shardAssigner;
        this.subscribedShardsState = new ArrayList<>();
        this.shardConsumersExecutor = createThreadPool(context.getTaskNameWithSubtasks());
        this.error = new AtomicReference<>();
        this.project = project;
        this.logClient = logClient;
        this.checkpointMode = checkpointMode;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (checkpointMode == CheckpointMode.PERIODIC) {
            commitInterval = LogUtil.getCommitIntervalMs(configProps);
            checkArgument(commitInterval > 0,
                    "Checkpoint commit interval must be positive: " + commitInterval);
            checkArgument(consumerGroup != null && !consumerGroup.isEmpty(),
                    "Missing parameter: " + ConfigConstants.LOG_CONSUMERGROUP);
        }
        this.consumerCache = new ConcurrentHashMap<>();
        this.logstores = logstores;
        this.logstorePattern = logstorePattern;
        this.subscribedLogstores = new HashSet<>();
        String stopTime = configProps.getProperty(ConfigConstants.STOP_TIME);
        if (stopTime != null && !stopTime.isEmpty()) {
            validateStopTime(stopTime);
            // Quit task on all shard reached stop time.
            exitAfterAllShardFinished = true;
        }
        this.memoryLimiter = memoryLimiter;
        this.queue = new LinkedBlockingQueue<>(
                PropertiesUtil.getInt(configProps, ConfigConstants.SOURCE_QUEUE_SIZE, DEFAULT_QUEUE_SIZE));
    }

    public boolean isRunning() {
        return running && !Thread.interrupted();
    }

    private static void validateStopTime(String stopTime) {
        try {
            Integer.parseInt(stopTime);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid " + ConfigConstants.STOP_TIME + ": " + stopTime);
        }
    }

    private static class GetDataThreadFactory implements ThreadFactory {
        private final String subtaskName;
        private final AtomicLong threadCount = new AtomicLong(0);

        GetDataThreadFactory(String subtaskName) {
            this.subtaskName = subtaskName;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Consumer-" + subtaskName + "-" + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

    private static ExecutorService createThreadPool(final String subtaskName) {
        return Executors.newCachedThreadPool(new GetDataThreadFactory(subtaskName));
    }

    private List<String> getLogstores() {
        return logstores != null ? logstores : logClient.listLogstores(project, logstorePattern);
    }

    private List<LogstoreShardMeta> listAssignedShards() throws Exception {
        List<String> logstores = getLogstores();
        List<LogstoreShardMeta> shardMetas = new ArrayList<>();
        for (String logstore : logstores) {
            List<Shard> shards = logClient.listShards(project, logstore);
            for (Shard shard : shards) {
                LogstoreShardMeta shardMeta = new LogstoreShardMeta(logstore, shard.GetShardId(), shard.getStatus());
                int hash = shardAssigner.assign(shardMeta, totalNumberOfSubtasks);
                int taskId = ((hash % totalNumberOfSubtasks) + totalNumberOfSubtasks) % totalNumberOfSubtasks;
                if (taskId == indexOfThisSubtask) {
                    shardMetas.add(shardMeta);
                }
            }
        }
        return shardMetas;
    }

    public List<LogstoreShardMeta> discoverNewShardsToSubscribe() throws Exception {
        List<LogstoreShardMeta> shardMetas = listAssignedShards();
        List<LogstoreShardMeta> newShards = new ArrayList<>();
        List<String> readonlyShards = new ArrayList<>();
        for (LogstoreShardMeta shard : shardMetas) {
            boolean isNew = true;
            String status = shard.getShardStatus();
            int shardID = shard.getShardId();
            for (LogstoreShardState state : subscribedShardsState) {
                LogstoreShardMeta shardMeta = state.getShardMeta();
                if (!shardMeta.getId().equals(shard.getId())) {
                    // Never subscribed before
                    continue;
                }
                if (shard.isReadOnly() && shardMeta.getEndCursor() == null) {
                    String endCursor = logClient.getEndCursor(project, shardMeta.getLogstore(), shardID);
                    LOG.info("Fetched end cursor [{}] for read only shard [{}]", endCursor, shardMeta.getId());
                    shardMeta.setEndCursor(endCursor);
                    shardMeta.setShardStatus(status);
                    readonlyShards.add(shardMeta.getId());
                }
                isNew = false;
                break;
            }
            if (isNew) {
                LOG.info("Subscribe new shard: {}, task: {}", shard.getId(), indexOfThisSubtask);
                newShards.add(shard);
            }
        }
        if (!readonlyShards.isEmpty()) {
            markConsumersAsReadOnly(readonlyShards);
        }
        return newShards;
    }

    private void markConsumersAsReadOnly(List<String> shards) {
        for (String shard : shards) {
            LOG.info("Mark shard {} as readonly", shard);
            ShardConsumer<T> consumer = consumerCache.get(shard);
            if (consumer != null) {
                consumer.markAsReadOnly();
            }
        }
    }

    private void createConsumerGroupIfNotExist(String logstore) {
        if (StringUtils.isNullOrWhitespaceOnly(consumerGroup)) {
            LOG.info("Consumer group is empty: [{}]", consumerGroup);
            return;
        }
        boolean exist = false;
        try {
            // TODO add get consumer group API
            exist = logClient.checkConsumerGroupExists(project, logstore, consumerGroup);
        } catch (Exception ex) {
            LOG.warn("Error checking consumer group exists {}", ex.getMessage());
            // do not throw exception here for bwc
        }
        if (!exist) {
            LOG.info("Creating consumer group {} for project {} logstore {}",
                    consumerGroup, project, logstore);
            try {
                logClient.createConsumerGroup(project, logstore, consumerGroup);
            } catch (Exception ex) {
                LOG.warn("Error creating consumer group {} - {}", consumerGroup, ex.getMessage());
            }
        }
    }

    public int registerNewSubscribedShard(LogstoreShardMeta shard, String checkpoint) {
        String logstore = shard.getLogstore();
        if (!subscribedLogstores.contains(logstore)) {
            subscribedLogstores.add(logstore);
            createConsumerGroupIfNotExist(logstore);
        }
        synchronized (checkpointLock) {
            subscribedShardsState.add(new LogstoreShardState(shard, checkpoint));
            return subscribedShardsState.size() - 1;
        }
    }

    public HashMap<LogstoreShardMeta, String> snapshotState() {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);
        HashMap<LogstoreShardMeta, String> stateSnapshot = new HashMap<>();
        for (LogstoreShardState shardWithState : subscribedShardsState) {
            stateSnapshot.put(shardWithState.getShardMeta(), shardWithState.getOffset());
        }
        return stateSnapshot;
    }

    private void createConsumerForShard(int index, LogstoreShardMeta shard) {
        ShardConsumer<T> consumer = new ShardConsumer<>(this, project, deserializer, index, configProps, logClient, recordEmitter);
        if (!running) {
            // Do not create consumer anymore
            return;
        }
        consumerCache.put(shard.getId(), consumer);
        shardConsumersExecutor.submit(consumer);
        numberOfActiveShards.incrementAndGet();
    }

    public void runFetcher() throws Exception {
        if (!running) {
            return;
        }
        startCommitThreadIfNeeded();
        long idleInterval = PropertiesUtil.getLong(configProps, ConfigConstants.SOURCE_IDLE_INTERVAL, DEFAULT_IDLE_INTERVAL);
        LOG.info("Starting record emitter, idle interval {}", idleInterval);
        recordEmitter = new RecordEmitter<>(queue, this, autoCommitter, idleInterval, memoryLimiter);
        shardConsumersExecutor.submit(recordEmitter);
        for (int index = 0; index < subscribedShardsState.size(); ++index) {
            LogstoreShardState shardState = subscribedShardsState.get(index);
            if (!shardState.isEndReached()) {
                LogstoreShardMeta shardMeta = shardState.getShardMeta();
                LOG.info("Start consumer for shard [{}]", shardMeta.getId());
                createConsumerForShard(index, shardMeta);
            }
        }
        final long discoveryIntervalMillis = LogUtil.getDiscoveryIntervalMs(configProps);
        if (numberOfActiveShards.get() == 0) {
            LOG.info("No shards were assigned to this task {}, mark idle", indexOfThisSubtask);
            sourceContext.markAsTemporarilyIdle();
        }
        while (running) {
            List<LogstoreShardMeta> newShardsDueToResharding = discoverNewShardsToSubscribe();
            for (LogstoreShardMeta shard : newShardsDueToResharding) {
                int newStateIndex = registerNewSubscribedShard(shard, null);
                LOG.info("discover new shard: {}, task: {}, total task: {}",
                        shard.toString(), indexOfThisSubtask, totalNumberOfSubtasks);
                createConsumerForShard(newStateIndex, shard);
            }
            if (exitAfterAllShardFinished && consumerCache.isEmpty()) {
                LOG.info("All shard consumers exited");
                break;
            }
            if (running && discoveryIntervalMillis != 0) {
                try {
                    cancelFuture.get(discoveryIntervalMillis, TimeUnit.MILLISECONDS);
                    LOG.debug("Cancelled discovery");
                } catch (TimeoutException iex) {
                    // timeout is expected when fetcher is not cancelled
                }
            }
        }

        // make sure all resources have been terminated before leaving
        try {
            awaitTermination();
        } catch (InterruptedException ie) {
            // If there is an original exception, preserve it, since that's more important/useful.
            this.error.compareAndSet(null, ie);
        }

        // any error thrown in the shard consumer threads will be thrown to the main thread
        Throwable throwable = this.error.get();
        if (throwable != null) {
            if (throwable instanceof Exception) {
                throw (Exception) throwable;
            } else if (throwable instanceof Error) {
                throw (Error) throwable;
            } else {
                throw new Exception(throwable);
            }
        }
    }

    private void startCommitThreadIfNeeded() {
        if (checkpointMode == CheckpointMode.PERIODIC) {
            autoCommitter = new CheckpointCommitter(logClient, commitInterval, project, consumerGroup);
            autoCommitter.start();
            LOG.info("Checkpoint periodic committer thread started");
        }
    }

    public void awaitTermination() throws InterruptedException {
        long begin = System.currentTimeMillis();
        while (!shardConsumersExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
            if (System.currentTimeMillis() - begin > 60000) {
                LOG.warn("Waiting executor terminating timeout.");
                break;
            }
            LOG.warn("Executor is still running, check again after 1s.");
        }
        LOG.warn("LogDataFetcher exit awaitTermination");
    }

    public void shutdownFetcher() {
        running = false;
        LOG.warn("Stopping all consumers..");
        for (ShardConsumer<T> consumer : consumerCache.values()) {
            consumer.cancel();
        }

        cancelFuture.complete(null);
        if (recordEmitter != null) {
            LOG.info("Waiting for recordEmitter idle.");
            try {
                recordEmitter.waitForIdle();
            } catch (Exception ex) {
                LOG.warn("Encountered exception waiting consumer idle.", ex);
            }
        }
        LOG.warn("Shutting down the shard consumer threads of subtask {}", indexOfThisSubtask);
        shardConsumersExecutor.shutdownNow();
        if (autoCommitter != null) {
            LOG.info("Stopping checkpoint committer thread.");
            autoCommitter.cancel();
        }
        LOG.warn("shutdownFetcher exited");
    }

    void emitRecordAndUpdateState(T record, long recordTimestamp, int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, recordTimestamp);
            LogstoreShardState state = subscribedShardsState.get(shardStateIndex);
            if (state != null) {
                state.setOffset(cursor);
            }
        }
    }

    void stopWithError(Throwable throwable) {
        if (this.error.compareAndSet(null, throwable)) {
            LOG.error("Stop fetcher due to exception", throwable);
            shutdownFetcher();
        }
    }

    void complete(String shardId) {
        int active = numberOfActiveShards.decrementAndGet();
        LOG.warn("Shard [{}] is finished, active {}.", shardId, active);
        consumerCache.remove(shardId);
        if (active <= 0) {
            LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
                    indexOfThisSubtask);
            sourceContext.markAsTemporarilyIdle();
        }
    }

    LogstoreShardState getShardState(int index) {
        return subscribedShardsState.get(index);
    }
}
