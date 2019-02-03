package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.model.LogDataFetcher;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import com.aliyun.openservices.log.flink.model.LogstoreShardState;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkLogConsumer<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointedFunction, CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogConsumer.class);

    private final Properties configProps;
    private transient LogDataFetcher<T> fetcher;
    private volatile boolean running = true;
    private static final String CURSOR_STATE_STORE_NAME = "LogStore-Shard-State";
    private transient ListState<Tuple2<LogstoreShardMeta, String>> cursorStateForCheckpoint;
    private final LogDeserializationSchema<T> deserializer;
    private transient HashMap<LogstoreShardMeta, String> cursorsToRestore;
    private String consumerGroupName = null;
    private LogClientProxy logClient;
    private String logProject = null;
    private String logStore = null;

    public FlinkLogConsumer(LogDeserializationSchema<T> deserializer, Properties configProps) {
        this.configProps = configProps;
        this.deserializer = deserializer;
        if (configProps.containsKey(ConfigConstants.LOG_CONSUMERGROUP)) {
            this.consumerGroupName = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        }
        this.logProject = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logStore = configProps.getProperty(ConfigConstants.LOG_LOGSTORE);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        this.logClient = new LogClientProxy(configProps.getProperty(ConfigConstants.LOG_ENDPOINT),
                configProps.getProperty(ConfigConstants.LOG_ACCESSSKEYID),
                configProps.getProperty(ConfigConstants.LOG_ACCESSKEY));
        LogDataFetcher<T> fetcher = new LogDataFetcher<T>(sourceContext, getRuntimeContext(), configProps, deserializer, logClient);
        if (consumerGroupName != null) {
            logClient.createConsumerGroup(fetcher.getLogProject(), fetcher.getLogStore(), consumerGroupName);
        }
        List<LogstoreShardMeta> newShards = fetcher.discoverNewShardsToSubscribe();
        for (LogstoreShardMeta shard : newShards) {
            if (cursorsToRestore != null) {
                if (cursorsToRestore.containsKey(shard)) {
                    fetcher.registerNewSubscribedShardState(new LogstoreShardState(shard, cursorsToRestore.get(shard)));
                } else {
                    fetcher.registerNewSubscribedShardState(new LogstoreShardState(shard, null));
                }
            } else {
                fetcher.registerNewSubscribedShardState(new LogstoreShardState(shard, null));
            }
        }
        if (!running) return;
        this.fetcher = fetcher;
        fetcher.runFetcher();
        fetcher.awaitTermination();
        sourceContext.close();
    }

    @Override
    public void cancel() {
        running = false;

        LogDataFetcher<T> fetcher = this.fetcher;
        this.fetcher = null;

        // this method might be called before the subtask actually starts running,
        // so we must check if the fetcher is actually created
        if (fetcher != null) {
            try {
                // interrupt the fetcher of any work
                fetcher.shutdownFetcher();
                fetcher.awaitTermination();
            } catch (Exception e) {
                LOG.warn("Error while closing log data fetcher", e);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.info("snapshotState() called on closed source; returning null.");
        } else {
            LOG.info("Snapshotting state ...");

            cursorStateForCheckpoint.clear();

            if (fetcher == null) {
                if (cursorsToRestore != null) {
                    for (Map.Entry<LogstoreShardMeta, String> entry : cursorsToRestore.entrySet()) {
                        // cursorsToRestore is the restored global union state;
                        // should only snapshot shards that actually belong to us

                        if (LogDataFetcher.isThisSubtaskShouldSubscribeTo(
                                entry.getKey(),
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                getRuntimeContext().getIndexOfThisSubtask())) {

                            cursorStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
                            if (consumerGroupName != null && logClient != null) {
                                logClient.updateCheckpoint(logProject, logStore, consumerGroupName, "flinkTask-" + getRuntimeContext().getIndexOfThisSubtask() + "Of" + getRuntimeContext().getNumberOfParallelSubtasks(), entry.getKey().getShardId(), entry.getValue());
                            }
                        }
                    }
                }
            } else {
                Map<LogstoreShardMeta, String> lastStateSnapshot = fetcher.snapshotState();

                if (LOG.isDebugEnabled()) {
                    StringBuilder strb = new StringBuilder();
                    for (Map.Entry<LogstoreShardMeta, String> entry : lastStateSnapshot.entrySet()) {
                        strb.append("shard: ").append(entry.getKey().toString()).append(", cursor: ").append(entry.getValue());
                    }
                    LOG.debug("Snapshotted state, last processed cursor: {}, checkpoint id: {}, timestamp: {}",
                            strb, context.getCheckpointId(), context.getCheckpointTimestamp());
                }

                for (Map.Entry<LogstoreShardMeta, String> entry : lastStateSnapshot.entrySet()) {
                    cursorStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
                    if (consumerGroupName != null && logClient != null) {
                        logClient.updateCheckpoint(logProject, logStore, consumerGroupName, "flinkTask-" + getRuntimeContext().getIndexOfThisSubtask() + "Of" + getRuntimeContext().getNumberOfParallelSubtasks(), entry.getKey().getShardId(), entry.getValue());
                    }
                }
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initializeState...");
        TypeInformation<Tuple2<LogstoreShardMeta, String>> shardsStateTypeInfo = new TupleTypeInfo<Tuple2<LogstoreShardMeta, String>>(
                TypeInformation.of(LogstoreShardMeta.class),
                TypeInformation.of(String.class));

        cursorStateForCheckpoint = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<Tuple2<LogstoreShardMeta, String>>(CURSOR_STATE_STORE_NAME, shardsStateTypeInfo));

        if (context.isRestored()) {
            if (cursorsToRestore == null) {
                cursorsToRestore = new HashMap<LogstoreShardMeta, String>();
                for (Tuple2<LogstoreShardMeta, String> cursor : cursorStateForCheckpoint.get()) {
                    LOG.info("initializeState, project: {}, logstore: {}, shard: {}, checkpoint: {}", logProject, logStore, cursor.f0.toString(), cursor.f1);
                    cursorsToRestore.put(cursor.f0, cursor.f1);
                    if (consumerGroupName != null && logClient != null) {
                        logClient.updateCheckpoint(logProject, logStore, consumerGroupName, "flinkTask-" + getRuntimeContext().getIndexOfThisSubtask() + "Of" + getRuntimeContext().getNumberOfParallelSubtasks(), cursor.f0.getShardId(), cursor.f1);
                    }
                }

                LOG.info("Setting restore state in the FlinkLogConsumer. Using the following offsets: {}",
                        cursorsToRestore);
            }
        } else {
            LOG.info("No restore state for FlinkLogConsumer.");
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
    }

    @Override
    public void close() throws Exception {
        cancel();
        super.close();
    }
}
