package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;

public class LogDataFetcher<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogDataFetcher.class);

    private final Properties configProps;
    private final LogDeserializationSchema<T> deserializationSchema;
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
    private final String logstore;
    private final CheckpointMode checkpointMode;
    private final String consumerGroup;
    private CheckpointCommitter autoCommitter;
    private long commitInterval;
    private Map<Integer, ShardConsumer<T>> activeConsumers;


    public LogDataFetcher(SourceFunction.SourceContext<T> sourceContext,
                          RuntimeContext runtimeContext,
                          Properties configProps,
                          LogDeserializationSchema<T> deserializationSchema,
                          LogClientProxy logClient,
                          CheckpointMode checkpointMode) {
        this.sourceContext = sourceContext;
        this.configProps = configProps;
        this.deserializationSchema = deserializationSchema;
        this.totalNumberOfSubtasks = runtimeContext.getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.subscribedShardsState = new LinkedList<LogstoreShardState>();
        this.shardConsumersExecutor = createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());
        this.error = new AtomicReference<Throwable>();
        this.project = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logstore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.logClient = logClient;
        this.checkpointMode = checkpointMode;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        if (checkpointMode == CheckpointMode.PERIODIC) {
            commitInterval = PropertiesUtil.getLong(
                    configProps,
                    ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS,
                    Consts.DEFAULT_COMMIT_INTERVAL_MILLIS);
            checkArgument(commitInterval > 0,
                    "Checkpoint commit interval must be positive: " + commitInterval);
            checkArgument(consumerGroup != null && !consumerGroup.isEmpty(),
                    "Missing parameter: " + ConfigConstants.LOG_CONSUMERGROUP);
        }
        this.activeConsumers = new HashMap<Integer, ShardConsumer<T>>();
    }

    public String getProject() {
        return project;
    }

    public String getLogstore() {
        return logstore;
    }

    public static boolean isThisSubtaskShouldSubscribeTo(LogstoreShardMeta shard,
                                                         int totalNumberOfSubtasks,
                                                         int indexOfThisSubtask) {
        return (Math.abs(shard.hashCode() % totalNumberOfSubtasks)) == indexOfThisSubtask;
    }

    private static ExecutorService createShardConsumersThreadPool(final String subtaskName) {
        return Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicLong threadCount = new AtomicLong(0);

            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("shardConsumers-" + subtaskName + "-thread-" + threadCount.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private List<LogstoreShardMeta> listAssignedShards() throws Exception {
        List<Shard> shards = logClient.listShards(project, logstore);
        List<LogstoreShardMeta> shardMetas = new ArrayList<LogstoreShardMeta>(shards.size());
        for (Shard shard : shards) {
            LogstoreShardMeta shardMeta = new LogstoreShardMeta(shard.GetShardId(), shard.getInclusiveBeginKey(), shard.getExclusiveEndKey(), shard.getStatus());
            if (isThisSubtaskShouldSubscribeTo(shardMeta, totalNumberOfSubtasks, indexOfThisSubtask)) {
                shardMetas.add(shardMeta);
            }
        }
        return shardMetas;
    }

    public List<LogstoreShardMeta> discoverNewShardsToSubscribe() throws Exception {
        List<LogstoreShardMeta> shardMetas = listAssignedShards();
        List<LogstoreShardMeta> newShards = new ArrayList<LogstoreShardMeta>();
        List<Integer> activeShards = new ArrayList<Integer>();
        for (LogstoreShardMeta shard : shardMetas) {
            boolean add = true;
            String status = shard.getShardStatus();
            int shardID = shard.getShardId();
            activeShards.add(shardID);
            for (LogstoreShardState state : subscribedShardsState) {
                LogstoreShardMeta shardMeta = state.getShardMeta();
                if (shardMeta.getShardId() == shardID) {
                    if (!shardMeta.getShardStatus().equalsIgnoreCase(status)
                            || shardMeta.needSetEndCursor()) {
                        String endCursor = logClient.getEndCursor(project, logstore, shardID);
                        LOG.info("The latest cursor of shard {} is {}", shardID, endCursor);
                        shardMeta.setEndCursor(endCursor);
                        shardMeta.setShardStatus(status);
                        LOG.info("change shard status to {}, shard: {}", status, shard.toString());
                        ShardConsumer<T> consumer = activeConsumers.get(shardID);
                        if (consumer != null) {
                            consumer.setReadOnly();
                            activeConsumers.remove(shardID);
                        }
                    }
                    add = false;
                    break;
                }
            }
            if (add) {
                LOG.info("Subscribe new shard: {}, task: {}", shard.toString(), indexOfThisSubtask);
                newShards.add(shard);
            }
        }
        Iterator<LogstoreShardState> iterator = subscribedShardsState.iterator();
        while (iterator.hasNext()) {
            LogstoreShardState state = iterator.next();
            int shardID = state.getShardMeta().getShardId();
            if (!activeShards.contains(shardID)) {
                // shard was not exist any more
                activeConsumers.remove(shardID);
                iterator.remove();
            }
        }
        return newShards;
    }

    public int registerNewSubscribedShardState(LogstoreShardState state) {
        synchronized (checkpointLock) {
            subscribedShardsState.add(state);
            return subscribedShardsState.size() - 1;
        }
    }

    public HashMap<LogstoreShardMeta, String> snapshotState() {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);
        HashMap<LogstoreShardMeta, String> stateSnapshot = new HashMap<LogstoreShardMeta, String>();
        for (LogstoreShardState shardWithState : subscribedShardsState) {
            stateSnapshot.put(shardWithState.getShardMeta(), shardWithState.getOffset());
        }
        return stateSnapshot;
    }

    private void createConsumerForShard(int index, int shardId) {
        ShardConsumer<T> consumer = new ShardConsumer<T>(this, deserializationSchema, index, configProps, logClient, autoCommitter);
        shardConsumersExecutor.submit(consumer);
        activeConsumers.put(shardId, consumer);
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
            if (!shardState.isFinished()) {
                createConsumerForShard(index, shardState.getShardMeta().getShardId());
            }
        }
        final long discoveryIntervalMillis = PropertiesUtil.getLong(
                configProps,
                ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS,
                Consts.DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS);
        if (numberOfActiveShards.get() == 0) {
            sourceContext.markAsTemporarilyIdle();
        }
        while (running) {
            List<LogstoreShardMeta> newShardsDueToResharding = discoverNewShardsToSubscribe();
            for (LogstoreShardMeta shard : newShardsDueToResharding) {
                LogstoreShardState shardState = new LogstoreShardState(shard, null);
                int newStateIndex = registerNewSubscribedShardState(shardState);
                LOG.info("discover new shard: {}, task: {}, taskcnt: {}", shardState.toString(), indexOfThisSubtask, totalNumberOfSubtasks);
                createConsumerForShard(newStateIndex, shardState.getShardMeta().getShardId());
            }
            if (running && discoveryIntervalMillis > 0) {
                try {
                    Thread.sleep(discoveryIntervalMillis);
                } catch (InterruptedException iex) {
                    // the sleep may be interrupted by shutdownFetcher()
                }
            }
        }
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
            autoCommitter = new CheckpointCommitter(logClient, commitInterval, this, project, logstore, consumerGroup);
            autoCommitter.start();
            LOG.info("Checkpoint periodic committer thread started");
        }
    }

    public void awaitTermination() throws InterruptedException {
        while (!shardConsumersExecutor.isTerminated()) {
            Thread.sleep(50);
        }
        LOG.warn("LogDataFetcher exit awaitTermination");
    }

    public void shutdownFetcher() {
        running = false;
        if (mainThread != null) {
            mainThread.interrupt(); // the main thread may be sleeping for the discovery interval
        }
        LOG.warn("Shutting down the shard consumer threads of subtask {} ...", indexOfThisSubtask);
        shardConsumersExecutor.shutdownNow();
        if (autoCommitter != null) {
            LOG.info("Stopping checkpoint committer thread.");
            autoCommitter.interrupt();
            autoCommitter.shutdown();
        }
    }

    void emitRecordAndUpdateState(T record, long recordTimestamp, int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, recordTimestamp);
            updateState(shardStateIndex, cursor);
        }
    }

    private void updateState(int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            LogstoreShardState state = subscribedShardsState.get(shardStateIndex);
            state.setOffset(cursor);
            if (state.isFinished()) {
                if (this.numberOfActiveShards.decrementAndGet() == 0) {
                    LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
                            indexOfThisSubtask);
                    sourceContext.markAsTemporarilyIdle();
                }
            }
        }
    }

    void stopWithError(Throwable throwable) {
        if (this.error.compareAndSet(null, throwable)) {
            LOG.error("LogDataFetcher stopWithError: {}", throwable.toString());
            shutdownFetcher();
        }
    }

    LogstoreShardState getShardState(int index) {
        return subscribedShardsState.get(index);
    }
}
