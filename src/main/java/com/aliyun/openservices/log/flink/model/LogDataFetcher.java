package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.ShardAssigner;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LogDataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(LogDataFetcher.class);
    public static final ShardAssigner DEFAULT_SHARD_ASSIGNER = new DefaultShardAssigner();

    private final Properties configProps;

    /**
     * Runtime context of the subtask that this fetcher was created in.
     */
    private final RuntimeContext runtimeContext;
    private final int totalNumberOfSubtasks;
    private final int indexOfThisSubtask;

    private final SourceFunction.SourceContext<SourceRecord> sourceContext;
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
    private Pattern logstorePattern;
    private List<String> logstores;
    private Set<String> subscribedLogstores;
    private final ShardAssigner shardAssigner;
    private final AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner;

    /**
     * The watermark related state for each shard consumer. Entries in this map will be created when shards
     * are discovered. After recovery, this shard map will be recreated, possibly with different shard index keys,
     * since those are transient and not part of checkpointed state.
     */
    private final ConcurrentHashMap<Integer, ShardWatermarkState> shardWatermarks = new ConcurrentHashMap<>();

    /**
     * The most recent watermark, calculated from the per shard watermarks. The initial value will never be emitted and
     * also apply after recovery. The fist watermark that will be emitted is derived from actually consumed records.
     * In case of recovery and replay, the watermark will rewind, consistent wth the shard consumer sequence.
     */
    private long lastWatermark = Long.MIN_VALUE;

    private final Map<String, ShardConsumer> consumerCache;
    private boolean exitAfterAllShardFinished = false;

    /**
     * The time span since last consumed record, after which a shard will be considered idle for purpose of watermark
     * calculation. A positive value will allow the watermark to progress even when some shards don't receive new records.
     */
    private long shardIdleIntervalMillis = ConfigConstants.DEFAULT_SHARD_IDLE_INTERVAL_MILLIS;


    public LogDataFetcher(SourceFunction.SourceContext<SourceRecord> sourceContext,
                          RuntimeContext context,
                          String project,
                          List<String> logstores,
                          Pattern logstorePattern,
                          Properties configProps,
                          LogClientProxy logClient,
                          CheckpointMode checkpointMode,
                          ShardAssigner shardAssigner,
                          AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner) {
        this.sourceContext = sourceContext;
        this.configProps = configProps;
        this.runtimeContext = context;
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
        this.periodicWatermarkAssigner = periodicWatermarkAssigner;
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

    /**
     * The wrapper that holds the watermark handling related parameters
     * of a record produced by the shard consumer thread.
     */
    private static class RecordWrapper extends TimestampedValue<SourceRecord> {
        long timestamp;
        Watermark watermark;

        private RecordWrapper(SourceRecord record, long timestamp) {
            super(record, timestamp);
            this.timestamp = timestamp;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
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
            ShardConsumer consumer = consumerCache.get(shard);
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

    /**
     * Register a new subscribed shard state.
     *
     * @param shard the new shard state that this fetcher is to be subscribed to
     */
    public int registerNewSubscribedShard(LogstoreShardMeta shard, String offset) {
        String logstore = shard.getLogstore();
        if (!subscribedLogstores.contains(logstore)) {
            subscribedLogstores.add(logstore);
            createConsumerGroupIfNotExist(logstore);
        }
        synchronized (checkpointLock) {
            subscribedShardsState.add(new LogstoreShardState(shard, offset));
            int shardStateIndex = subscribedShardsState.size() - 1;

            // track all discovered shards for watermark determination
            ShardWatermarkState sws = shardWatermarks.get(shardStateIndex);
            if (sws == null) {
                sws = new ShardWatermarkState();
                try {
                    sws.periodicWatermarkAssigner = InstantiationUtil.clone(periodicWatermarkAssigner);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate new WatermarkAssigner", e);
                }
                sws.emitQueue = new RecordQueue<RecordWrapper>() {
                    @Override
                    public void put(RecordWrapper record) {
                    }

                    @Override
                    public int getSize() {
                        return 0;
                    }

                    @Override
                    public RecordWrapper peek() {
                        return null;
                    }
                };
                sws.lastUpdated = getCurrentTimeMillis();
                sws.lastRecordTimestamp = Long.MIN_VALUE;
                shardWatermarks.put(shardStateIndex, sws);
            }
            return shardStateIndex;
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
        ShardConsumer consumer = new ShardConsumer(this, index, configProps, logClient, autoCommitter);
        if (!running) {
            // Do not create consumer any more
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
        this.mainThread = Thread.currentThread();
        startCommitThreadIfNeeded();
        for (int index = 0; index < subscribedShardsState.size(); ++index) {
            LogstoreShardState shardState = subscribedShardsState.get(index);
            if (!shardState.isIdle()) {
                LogstoreShardMeta shardMeta = shardState.getShardMeta();
                LOG.info("Start consumer for shard [{}]", shardMeta.getId());
                createConsumerForShard(index, shardMeta);
            }
        }

        // start periodic watermark emitter, if a watermark assigner was configured
        if (periodicWatermarkAssigner != null) {
            long periodicWatermarkIntervalMillis = runtimeContext.getExecutionConfig().getAutoWatermarkInterval();
            if (periodicWatermarkIntervalMillis > 0) {
                ProcessingTimeService timerService = ((StreamingRuntimeContext) runtimeContext).getProcessingTimeService();
                LOG.info("Starting periodic watermark emitter with interval {}", periodicWatermarkIntervalMillis);
                new PeriodicWatermarkEmitter(timerService, periodicWatermarkIntervalMillis).start();
            }
            this.shardIdleIntervalMillis = Long.parseLong(
                    configProps.getProperty(ConfigConstants.SHARD_IDLE_INTERVAL_MILLIS,
                            Long.toString(ConfigConstants.DEFAULT_SHARD_IDLE_INTERVAL_MILLIS)));
            // run record emitter in separate thread since main thread is used for discovery
        }

        final long discoveryIntervalMs = LogUtil.getDiscoveryIntervalMs(configProps);
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
            if (exitAfterAllShardFinished) {
                if (consumerCache.isEmpty()) {
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

        // make sure all resources have been terminated before leaving
        try {
            awaitTermination();
        } catch (InterruptedException ie) {
            // If there is an original exception, preserve it, since that's more important/useful.
            this.error.compareAndSet(null, ie);
        }

        stopBackgroundThreads();
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
            autoCommitter = new CheckpointCommitter(logClient, commitInterval, project, consumerGroup);
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
        for (ShardConsumer consumer : consumerCache.values()) {
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

    protected void emitRecordAndUpdateState(SourceRecord record, int shardStateIndex, String offset) {
        ShardWatermarkState sws = shardWatermarks.get(shardStateIndex);
        Preconditions.checkNotNull(
                sws, "shard watermark state initialized in registerNewSubscribedShardState");
        Watermark watermark = null;
        long timestamp = record.getTimestamp() * 1000;
        if (sws.periodicWatermarkAssigner != null) {
            timestamp = sws.periodicWatermarkAssigner.extractTimestamp(record, sws.lastRecordTimestamp);
            // track watermark per record since extractTimestamp has side effect
            watermark = sws.periodicWatermarkAssigner.getCurrentWatermark();
        }
        sws.lastRecordTimestamp = timestamp;
        sws.lastUpdated = getCurrentTimeMillis();
        emitRecordAndUpdateState(record, timestamp, shardStateIndex, watermark, offset);
    }

    /**
     * Atomic operation to collect a record and update state to the sequence number of the record.
     * This method is called from the record emitter.
     *
     * <p>Responsible for tracking per shard watermarks and emit timestamps extracted from
     * the record, when a watermark assigner was configured.
     */
    private void emitRecordAndUpdateState(SourceRecord record,
                                          long timestamp,
                                          int shardStateIndex,
                                          Watermark watermark,
                                          String offset) {
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, timestamp);
            ShardWatermarkState sws = shardWatermarks.get(shardStateIndex);
            sws.lastEmittedRecordWatermark = watermark;
            if (offset != null) {
                updateState(shardStateIndex, offset);
            }
        }
    }

    private void updateState(int shardStateIndex, String offset) {
        LogstoreShardState state = subscribedShardsState.get(shardStateIndex);
        state.setOffset(offset);
        if (state.isIdle() && this.numberOfActiveShards.decrementAndGet() == 0) {
            LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
                    indexOfThisSubtask);
            sourceContext.markAsTemporarilyIdle();
        }
    }

    /**
     * Return the current system time. Allow tests to override this to simulate progress for watermark
     * logic.
     *
     * @return current processing time
     */
    @VisibleForTesting
    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Called periodically to emit a watermark. Checks all shards for the current event time
     * watermark, and possibly emits the next watermark.
     *
     * <p>Shards that have not received an update for a certain interval are considered inactive so as
     * to not hold back the watermark indefinitely. When all shards are inactive, the subtask will be
     * marked as temporarily idle to not block downstream operators.
     */
    @VisibleForTesting
    protected void emitWatermark() {
        LOG.debug("Evaluating watermark for subtask {} time {}", indexOfThisSubtask, getCurrentTimeMillis());
        long potentialWatermark = Long.MAX_VALUE;
        long potentialNextWatermark = Long.MAX_VALUE;
        long idleTime =
                (shardIdleIntervalMillis > 0)
                        ? getCurrentTimeMillis() - shardIdleIntervalMillis
                        : Long.MAX_VALUE;

        for (Map.Entry<Integer, ShardWatermarkState> e : shardWatermarks.entrySet()) {
            Watermark w = e.getValue().lastEmittedRecordWatermark;
            // consider only active shards, or those that would advance the watermark
            if (w != null && (e.getValue().lastUpdated >= idleTime
                    || e.getValue().emitQueue.getSize() > 0
                    || w.getTimestamp() > lastWatermark)) {
                potentialWatermark = Math.min(potentialWatermark, w.getTimestamp());
                // for sync, use the watermark of the next record, when available
                // otherwise watermark may stall when record is blocked by synchronization
                RecordQueue<RecordWrapper> q = e.getValue().emitQueue;
                RecordWrapper nextRecord = q.peek();
                Watermark nextWatermark = (nextRecord != null) ? nextRecord.watermark : w;
                potentialNextWatermark = Math.min(potentialNextWatermark, nextWatermark.getTimestamp());
            }
        }

        // advance watermark if possible (watermarks can only be ascending)
        if (potentialWatermark == Long.MAX_VALUE) {
            if (shardWatermarks.isEmpty() || shardIdleIntervalMillis > 0) {
                LOG.info("No active shard for subtask {}, marking the source idle.",
                        indexOfThisSubtask);
                // no active shard, signal downstream operators to not wait for a watermark
                sourceContext.markAsTemporarilyIdle();
            }
        } else {
            if (potentialWatermark > lastWatermark) {
                LOG.debug("Emitting watermark {} from subtask {}",
                        potentialWatermark,
                        indexOfThisSubtask);
                sourceContext.emitWatermark(new Watermark(potentialWatermark));
                lastWatermark = potentialWatermark;
            }
        }
    }

    /**
     * Accepts records from readers.
     *
     * @param <T>
     */
    public interface RecordQueue<T> {
        void put(T record) throws InterruptedException;

        int getSize();

        T peek();
    }


    /**
     * Per shard tracking of watermark and last activity.
     */
    private static class ShardWatermarkState {
        private AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner;
        private RecordQueue<RecordWrapper> emitQueue;
        private volatile long lastRecordTimestamp;
        private volatile long lastUpdated;
        private volatile Watermark lastEmittedRecordWatermark;
    }

    /**
     * The periodic watermark emitter. In its given interval, it checks all shards for the current
     * event time watermark, and possibly emits the next watermark.
     */
    private class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

        private final ProcessingTimeService timerService;
        private final long interval;

        PeriodicWatermarkEmitter(ProcessingTimeService timerService, long autoWatermarkInterval) {
            this.timerService = checkNotNull(timerService);
            this.interval = autoWatermarkInterval;
        }

        public void start() {
            LOG.debug("registering periodic watermark timer with interval {}", interval);
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }

        @Override
        public void onProcessingTime(long timestamp) {
            emitWatermark();
            // schedule the next watermark
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }
    }

    void stopWithError(Throwable throwable) {
        if (this.error.compareAndSet(null, throwable)) {
            LOG.error("Stop fetcher due to exception", throwable);
            cancel();
        }
    }

    void onShardFinished(String shardId) {
        int active = numberOfActiveShards.decrementAndGet();
        LOG.warn("Shard [{}] has no more data to read, active {}.", shardId, active);
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
