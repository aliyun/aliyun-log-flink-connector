package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.ShardAssigner;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

public class LogDataFetcher<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogDataFetcher.class);
    public static final ShardAssigner DEFAULT_SHARD_ASSIGNER = new DefaultShardAssigner();

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
    private volatile Thread mainThread;
    private volatile boolean running = true;
    private volatile List<LogstoreShardState> subscribedShardsState;
    private final String project;
    private final CheckpointMode checkpointMode;
    private final String consumerGroup;
    private CheckpointCommitter autoCommitter;
    private long commitInterval;
    private ReadWriteLock rwLock;
    private Map<Integer, ShardConsumer<T>> allConsumers;
    private Pattern logstorePattern;
    private List<String> logstores;
    private Set<String> subscribedLogstores;
    private final ShardAssigner shardAssigner;
    private boolean exitAfterAllShardFinished = false;

    public LogDataFetcher(SourceFunction.SourceContext<T> sourceContext,
                          RuntimeContext context,
                          String project,
                          List<String> logstores,
                          Pattern logstorePattern,
                          Properties configProps,
                          LogDeserializationSchema<T> deserializer,
                          LogClientProxy logClient,
                          CheckpointMode checkpointMode,
                          ShardAssigner shardAssigner) {
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
        this.rwLock = new ReentrantReadWriteLock();
        this.allConsumers = new HashMap<>();
        this.logstores = logstores;
        this.logstorePattern = logstorePattern;
        this.subscribedLogstores = new HashSet<>();
        String stopTime = configProps.getProperty(ConfigConstants.STOP_TIME);
        if (stopTime != null && !stopTime.isEmpty()) {
            validateStopTime(stopTime);
            // Quit task on all shard reached stop time.
            exitAfterAllShardFinished = true;
        }
    }

    private static void validateStopTime(String stopTime) {
        try {
            Integer.parseInt(stopTime);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid " + ConfigConstants.STOP_TIME + ": " + stopTime);
        }
    }

    public String getProject() {
        return project;
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
            shards.sort((o1, o2) -> {
                int scoreO1 = o1.getCreateTime() + o1.GetShardId();
                int scoreO2 = o2.getCreateTime() + o2.GetShardId();
                return scoreO1 - scoreO2;
            });
            for (int shardIndex = 0; shardIndex < shards.size(); shardIndex++) {
                Shard shard = shards.get(shardIndex);
                LogstoreShardMeta shardMeta = new LogstoreShardMeta(logstore, shard.GetShardId(), shardIndex, shard.getStatus());
                if (shardAssigner.assign(shardMeta, totalNumberOfSubtasks) % totalNumberOfSubtasks == indexOfThisSubtask) {
                    shardMetas.add(shardMeta);
                }
            }

        }
        return shardMetas;
    }

    public List<LogstoreShardMeta> discoverNewShardsToSubscribe() throws Exception {
        List<LogstoreShardMeta> shardMetas = listAssignedShards();
        List<LogstoreShardMeta> newShards = new ArrayList<>();
        List<Integer> readonlyShards = new ArrayList<>();
        for (LogstoreShardMeta shard : shardMetas) {
            boolean add = true;
            String status = shard.getShardStatus();
            int shardID = shard.getShardId();
            for (LogstoreShardState state : subscribedShardsState) {
                LogstoreShardMeta shardMeta = state.getShardMeta();
                if (!shardMeta.equals(shard)) {
                    // Never subscribed before
                    continue;
                }
                if (!shardMeta.getShardStatus().equalsIgnoreCase(status)
                        || shardMeta.needSetEndCursor()) {
                    String endCursor = logClient.getEndCursor(project, shardMeta.getLogstore(), shardID);
                    shardMeta.setEndCursor(endCursor);
                    shardMeta.setShardStatus(status);
                    readonlyShards.add(shardMeta.getShardId());
                }
                add = false;
                break;
            }
            if (add) {
                LOG.info("Subscribe new shard: {}, task: {}", shard.toString(), indexOfThisSubtask);
                newShards.add(shard);
            }
        }
        if (!readonlyShards.isEmpty()) {
            markConsumersAsReadOnly(readonlyShards);
        }
        return newShards;
    }

    private void markConsumersAsReadOnly(List<Integer> shards) {
        rwLock.readLock();
        for (Integer shard : shards) {
            LOG.info("Mark shard {} as readonly", shard);
            ShardConsumer<T> consumer = allConsumers.get(shard);
            if (consumer != null) {
                consumer.markAsReadOnly();
            }
        }
    }

    private void createConsumerGroupIfNotExist(String logstore) {
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
        ShardConsumer<T> consumer = new ShardConsumer<>(this, deserializer, index, configProps, logClient, autoCommitter);
        rwLock.writeLock();
        if (!running) {
            // Do not create consumer any more
            return;
        }
        allConsumers.put(shard.getShardId(), consumer);
        shardConsumersExecutor.submit(consumer);
        numberOfActiveShards.incrementAndGet();
    }

    public void runFetcher() throws Exception {
        if (!running) {
            return;
        }
        this.mainThread = Thread.currentThread();
        startCommitThreadIfNeeded();
        for (int index = 0; index < subscribedShardsState.size(); ++index) {
            LogstoreShardState shardState = subscribedShardsState.get(index);
            if (shardState.hasMoreData()) {
                createConsumerForShard(index, shardState.getShardMeta());
            }
        }
        final long discoveryIntervalMs = LogUtil.getDiscoveryIntervalMs(configProps);
        if (numberOfActiveShards.get() == 0) {
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
            if (exitAfterAllShardFinished) {
                rwLock.readLock();
                if (allConsumers.isEmpty()) {
                    LOG.info("All shard consumers exited");
                    break;
                }
            }
            if (running && discoveryIntervalMs > 0) {
                try {
                    Thread.sleep(discoveryIntervalMs);
                } catch (InterruptedException iex) {
                    // the sleep may be interrupted by shutdownFetcher()
                }
            }
        }
        stopBackgroundThreads();
        awaitTermination();
        Throwable throwable = this.error.get();
        if (throwable != null) {
            if (throwable instanceof LogException) {
                throw (LogException) throwable;
            } else if (throwable instanceof Exception) {
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
            autoCommitter = new CheckpointCommitter(logClient, commitInterval, this, project, consumerGroup);
            autoCommitter.start();
            LOG.info("Checkpoint periodic committer thread started");
        }
    }

    public void awaitTermination() throws InterruptedException {
        while (!shardConsumersExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
            LOG.warn("Executor is still running, check again after 1s.");
        }
        LOG.warn("LogDataFetcher exit awaitTermination");
    }

    public void cancel() {
        running = false;
        if (mainThread != null) {
            // the main thread may be sleeping for the discovery interval
            mainThread.interrupt();
        }
    }

    private void stopAllConsumers() {
        LOG.warn("Stopping all consumers..");
        rwLock.readLock();
        for (ShardConsumer<T> consumer : allConsumers.values()) {
            consumer.stop();
        }
    }

    private void stopBackgroundThreads() {
        stopAllConsumers();
        LOG.warn("Shutting down the shard consumer threads of subtask {}", indexOfThisSubtask);
        shardConsumersExecutor.shutdownNow();
        if (autoCommitter != null) {
            LOG.info("Stopping checkpoint committer thread.");
            autoCommitter.shutdown();
            autoCommitter.interrupt();
        }
    }

    void emitRecordAndUpdateState(T record, long recordTimestamp, int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, recordTimestamp);
            LogstoreShardState state = subscribedShardsState.get(shardStateIndex);
            state.setOffset(cursor);
            if (state.hasMoreData()) {
                return;
            }
            if (this.numberOfActiveShards.decrementAndGet() == 0) {
                LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
                        indexOfThisSubtask);
                sourceContext.markAsTemporarilyIdle();
            }
        }
    }

    void stopWithError(Throwable throwable) {
        if (this.error.compareAndSet(null, throwable)) {
            LOG.error("Stop fetcher due to exception", throwable);
            cancel();
        }
    }

    void onShardFinished(int shard) {
        LOG.info("Remove shard {} from fetcher", shard);
        rwLock.writeLock();
        allConsumers.remove(shard);
    }

    LogstoreShardState getShardState(int index) {
        return subscribedShardsState.get(index);
    }
}
