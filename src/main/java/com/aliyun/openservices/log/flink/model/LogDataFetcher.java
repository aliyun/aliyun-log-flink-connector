package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.exception.LogException;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LogDataFetcher<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogDataFetcher.class);

    private final Properties configProps;
    private final LogDeserializationSchema<T> deserializationSchema;
    private final RuntimeContext runtimeContext;
    private final int totalNumberOfConsumerSubtasks;
    private final int indexOfThisConsumerSubtask;
    private final SourceFunction.SourceContext<T> sourceContext;
    private List<LogstoreShardState> subscribedShardsState;
    private final Object checkpointLock;
    private final ExecutorService shardConsumersExecutor;
    private final AtomicInteger numberOfActiveShards = new AtomicInteger(0);
    private volatile Thread mainThread;
    private final AtomicReference<Throwable> error;
    private final LogClientProxy logClient;
    private volatile boolean running = true;
    private final String logProject;
    private final String logStore;
    public LogDataFetcher(SourceFunction.SourceContext<T> sourceContext,
                          RuntimeContext runtimeContext,
                          Properties configProps,
                          LogDeserializationSchema<T> deserializationSchema, LogClientProxy logClient) {
        this.sourceContext = sourceContext;
        this.runtimeContext = runtimeContext;
        this.configProps = configProps;
        this.deserializationSchema = deserializationSchema;
        this.totalNumberOfConsumerSubtasks = runtimeContext.getNumberOfParallelSubtasks();
        this.indexOfThisConsumerSubtask = runtimeContext.getIndexOfThisSubtask();
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.subscribedShardsState = new LinkedList<LogstoreShardState>();
        this.shardConsumersExecutor = createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());
        this.error = new AtomicReference<Throwable>();
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
        this.logClient = logClient;
    }

    public String getLogProject() {
        return logProject;
    }

    public String getLogStore() {
        return logStore;
    }

    public static boolean isThisSubtaskShouldSubscribeTo(LogstoreShardMeta shard,
                                                         int totalNumberOfConsumerSubtasks,
                                                         int indexOfThisConsumerSubtask) {
        return (Math.abs(shard.hashCode() % totalNumberOfConsumerSubtasks)) == indexOfThisConsumerSubtask;
    }

    private static ExecutorService createShardConsumersThreadPool(final String subtaskName) {
        return Executors.newCachedThreadPool(new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                final AtomicLong threadCount = new AtomicLong(0);
                Thread thread = new Thread(runnable);
                thread.setName("shardConsumers-" + subtaskName + "-thread-" + threadCount.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
    }
    public List<LogstoreShardMeta> discoverNewShardsToSubscribe() throws LogException {
        List<LogstoreShardMeta> subShards = new ArrayList<LogstoreShardMeta>();
        for(LogstoreShardMeta shard: logClient.listShards(logProject, logStore)){
            if(isThisSubtaskShouldSubscribeTo(shard, totalNumberOfConsumerSubtasks, indexOfThisConsumerSubtask)){
                boolean add = true;
                for(LogstoreShardState state: subscribedShardsState){
                    if(state.getShardMeta().getShardId() == shard.getShardId()){
                        if(state.getShardMeta().getShardStatus().compareToIgnoreCase(shard.getShardStatus()) != 0
                                || (shard.getShardStatus().compareToIgnoreCase(Consts.READONLY_SHARD_STATUS) == 0 && state.getShardMeta().getEndCursor() == null)) {
                            String endCursor = logClient.getCursor(logProject, logStore, shard.getShardId(), Consts.LOG_END_CURSOR, "");
                            state.getShardMeta().setEndCursor(endCursor);
                            state.getShardMeta().setShardStatus(Consts.READONLY_SHARD_STATUS);
                            LOG.info("change shard status, shard: {}", shard.toString());
                        }
                        add = false;
                        break;
                    }
                }
                if(add){
                    LOG.info("Subscribe new shard: {}, task: {}", shard.toString(), indexOfThisConsumerSubtask);
                    subShards.add(shard);
                }
            }
        }
        return subShards;
    }
    public int registerNewSubscribedShardState(LogstoreShardState state){
        synchronized (checkpointLock){
            subscribedShardsState.add(state);
            return subscribedShardsState.size() - 1;
        }
    }
    public HashMap<LogstoreShardMeta, String> snapshotState() {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);
        HashMap<LogstoreShardMeta, String> stateSnapshot = new HashMap<LogstoreShardMeta, String>();
        for (LogstoreShardState shardWithState : subscribedShardsState) {
            stateSnapshot.put(shardWithState.getShardMeta(), shardWithState.getLastConsumerCursor());
        }
        return stateSnapshot;
    }
    public void runFetcher() throws Exception {
        if(!running) return;
        this.mainThread = Thread.currentThread();
        for(int index = 0; index < subscribedShardsState.size(); ++index){
            LogstoreShardState shardState = subscribedShardsState.get(index);
            if(shardState.hasMoreData()){
                numberOfActiveShards.incrementAndGet();
                shardConsumersExecutor.submit(new ShardConsumer(this, deserializationSchema, index, configProps, logClient));
            }
        }
        final long discoveryIntervalMillis = Long.valueOf(configProps.getProperty(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, Long.toString(Consts.DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS)));
        if(numberOfActiveShards.get() == 0)
            sourceContext.markAsTemporarilyIdle();
        while(running){
            List<LogstoreShardMeta> newShardsDueToResharding =  discoverNewShardsToSubscribe();
            for(LogstoreShardMeta shard: newShardsDueToResharding){
                LogstoreShardState shardState = new LogstoreShardState(shard, null);
                int newStateIndex = registerNewSubscribedShardState(shardState);
                shardConsumersExecutor.submit(new ShardConsumer<T>(this, deserializationSchema, newStateIndex, configProps, logClient));
                LOG.info("discover new shard: {}, task: {}, taskcnt: {}", shardState.toString(), indexOfThisConsumerSubtask, totalNumberOfConsumerSubtasks);
            }
            if (running && discoveryIntervalMillis != 0) {
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
            if(throwable instanceof LogException){
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
    public void awaitTermination() throws InterruptedException{
        while (!shardConsumersExecutor.isTerminated()) {
            Thread.sleep(50);
        }
        LOG.warn("LogdataFetcher exit awaitTermination");
    }
    public void shutdownFetcher() {
        running = false;

        if (mainThread != null) {
            mainThread.interrupt(); // the main thread may be sleeping for the discovery interval
        }

        LOG.warn("Shutting down the shard consumer threads of subtask {} ...", indexOfThisConsumerSubtask);
        shardConsumersExecutor.shutdownNow();
    }
    public void emitRecordAndUpdateState(T record, long recordTimestamp, int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, recordTimestamp);
            updateState(shardStateIndex, cursor);
        }
    }
    protected void updateState(int shardStateIndex, String cursor) {
        synchronized (checkpointLock) {
            LogstoreShardState state = subscribedShardsState.get(shardStateIndex);
            state.setLastConsumerCursor(cursor);
            if(state.getShardMeta().getShardStatus().compareToIgnoreCase(Consts.READONLY_SHARD_STATUS) == 0 && cursor.compareTo(state.getShardMeta().getEndCursor()) == 0){
                if (this.numberOfActiveShards.decrementAndGet() == 0) {
                    LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
                            indexOfThisConsumerSubtask);
                    sourceContext.markAsTemporarilyIdle();
                }
            }
        }
    }
    public void stopWithError(Throwable throwable) {
        if (this.error.compareAndSet(null, throwable)) {
            LOG.error("LogDataFetcher stopWithError: {}", throwable.toString());
            shutdownFetcher();
        }
    }
    public LogstoreShardState getShardState(int index){
        return subscribedShardsState.get(index);
    }
}
