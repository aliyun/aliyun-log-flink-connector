package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.data.FastLogGroupList;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.LogDataFetcher;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import com.aliyun.openservices.log.flink.model.LogstoreShardHandle;
import com.aliyun.openservices.log.flink.model.SourceRecord;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkLogConsumer extends RichParallelSourceFunction<SourceRecord> implements ResultTypeQueryable<SourceRecord>,
        CheckpointedFunction, CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogConsumer.class);
    private static final long serialVersionUID = 7835636734161627680L;

    private static final String CURSOR_STATE_STORE_NAME = "LogStore-Shard-State";

    private final Properties configProps;
    private transient LogDataFetcher fetcher;
    private volatile boolean running = true;
    private transient ListState<Tuple2<LogstoreShardHandle, String>> cursorStateForCheckpoint;
    private transient HashMap<LogstoreShardHandle, String> cursorsToRestore;
    private final String consumerGroup;
    private LogClientProxy logClient;
    private final String project;
    private List<String> logstores;
    private Pattern logstorePattern;
    private final CheckpointMode checkpointMode;
    private ShardAssigner shardAssigner = LogDataFetcher.DEFAULT_SHARD_ASSIGNER;
    private AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner;

    @Deprecated
    public FlinkLogConsumer(Properties configProps) {
        this.configProps = configProps;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logstores = Collections.singletonList(configProps.getProperty(ConfigConstants.LOG_LOGSTORE));
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
    }

    public FlinkLogConsumer(String project, List<String> logstores, Properties configProps) {
        this.configProps = configProps;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = project;
        this.logstores = logstores;
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
    }

    public FlinkLogConsumer(String project, String logstore, Properties configProps) {
        this(project, Collections.singletonList(logstore), configProps);
    }

    public FlinkLogConsumer(String project, Pattern logstorePattern, Properties configProps) {
        this.configProps = configProps;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = project;
        this.logstorePattern = logstorePattern;
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
    }

    private void createClientIfNeeded() {
        if (logClient != null) {
            return;
        }
        final String userAgent = configProps.getProperty(ConfigConstants.LOG_USER_AGENT,
                Consts.LOG_CONNECTOR_USER_AGENT);
        logClient = new LogClientProxy(
                configProps.getProperty(ConfigConstants.LOG_ENDPOINT),
                configProps.getProperty(ConfigConstants.LOG_ACCESSSKEYID),
                configProps.getProperty(ConfigConstants.LOG_ACCESSKEY),
                userAgent);
    }

    public ShardAssigner getShardAssigner() {
        return shardAssigner;
    }

    public void setShardAssigner(ShardAssigner shardAssigner) {
        this.shardAssigner = shardAssigner;
    }

    public AssignerWithPeriodicWatermarks<SourceRecord> getPeriodicWatermarkAssigner() {
        return periodicWatermarkAssigner;
    }

    /**
     * Set the assigner that will extract the timestamp from {@link SourceRecord} and calculate the
     * watermark.
     * @param periodicWatermarkAssigner periodic watermark assigner
     */
    public void setPeriodicWatermarkAssigner(
            AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner) {
        this.periodicWatermarkAssigner = periodicWatermarkAssigner;
        ClosureCleaner.clean(this.periodicWatermarkAssigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    @Override
    public void run(SourceContext<SourceRecord> sourceContext) throws Exception {
        createClientIfNeeded();
        final RuntimeContext ctx = getRuntimeContext();
        LOG.debug("NumberOfTotalTask={}, IndexOfThisSubtask={}", ctx.getNumberOfParallelSubtasks(), ctx.getIndexOfThisSubtask());
        LogDataFetcher fetcher = new LogDataFetcher(sourceContext,
                ctx, project,
                logstores, logstorePattern,
                configProps,
                logClient,
                checkpointMode,
                shardAssigner,
                periodicWatermarkAssigner);
        List<LogstoreShardHandle> newShards = fetcher.discoverNewShardsToSubscribe();
        for (LogstoreShardHandle shard : newShards) {
            String checkpoint = null;
            if (cursorsToRestore != null && cursorsToRestore.containsKey(shard)) {
                checkpoint = cursorsToRestore.get(shard);
            }
            fetcher.registerNewSubscribedShardState(shard, checkpoint);
        }
        if (!running) {
            return;
        }
        this.fetcher = fetcher;
        fetcher.runFetcher();
        fetcher.awaitTermination();
        logClient.close();
        sourceContext.close();
    }

    @Override
    public void cancel() {
        running = false;

        LogDataFetcher fetcher = this.fetcher;
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
        if (logClient != null) {
            logClient.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.info("snapshotState() called on closed source");
            return;
        }

        LOG.info("Snapshotting state ...");
        cursorStateForCheckpoint.clear();
        createClientIfNeeded();
        if (fetcher == null) {
            if (cursorsToRestore == null)
                return;
            final RuntimeContext ctx = getRuntimeContext();
            int numberOfParallelTasks = ctx.getNumberOfParallelSubtasks();
            int indexOfThisTask = ctx.getIndexOfThisSubtask();
            for (Map.Entry<LogstoreShardHandle, String> entry : cursorsToRestore.entrySet()) {
                // cursorsToRestore is the restored global union state;
                // should only snapshot shards that actually belong to us
                if (shardAssigner.assign(entry.getKey(), numberOfParallelTasks) % numberOfParallelTasks == indexOfThisTask) {
                    // Save to local state only. No need to sync with remote server
                    cursorStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
                }
            }
            return;
        }
        Map<LogstoreShardHandle, String> snapshotState = fetcher.snapshotState();
        if (LOG.isDebugEnabled()) {
            StringBuilder strb = new StringBuilder();
            for (Map.Entry<LogstoreShardHandle, String> entry : snapshotState.entrySet()) {
                strb.append("shard: ").append(entry.getKey().getShardId()).append(", cursor: ").append(entry.getValue());
            }
            LOG.debug("Snapshotted state, last processed cursor: {}, checkpoint id: {}, timestamp: {}",
                    strb, context.getCheckpointId(), context.getCheckpointTimestamp());
        }
        for (Map.Entry<LogstoreShardHandle, String> entry : snapshotState.entrySet()) {
            updateCursorState(entry.getKey(), entry.getValue());
        }
    }

    private void updateCursorState(LogstoreShardHandle shardMeta, String cursor) throws Exception {
        cursorStateForCheckpoint.add(Tuple2.of(shardMeta, cursor));
        if (cursor != null && consumerGroup != null && checkpointMode == CheckpointMode.ON_CHECKPOINTS) {
            updateCheckpoint(shardMeta, cursor);
        }
    }

    private void updateCheckpoint(LogstoreShardHandle meta, String cursor) throws Exception {
        logClient.updateCheckpoint(project, meta.getLogstore(),
                consumerGroup, meta.getShardId(), meta.isReadOnly(),
                cursor);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("Initializing state from Flink state");

        TypeInformation<Tuple2<LogstoreShardHandle, String>> shardsStateTypeInfo = new TupleTypeInfo<Tuple2<LogstoreShardHandle, String>>(
                TypeInformation.of(LogstoreShardHandle.class),
                TypeInformation.of(String.class));
        cursorStateForCheckpoint = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>(CURSOR_STATE_STORE_NAME, shardsStateTypeInfo));
        if (!context.isRestored()) {
            LOG.info("No state restored for FlinkLogConsumer.");
            return;
        }
        if (cursorsToRestore != null) {
            LOG.info("Flink state has been restored already.");
            return;
        }
        createClientIfNeeded();
        cursorsToRestore = new HashMap<>();
        for (Tuple2<LogstoreShardHandle, String> cursor : cursorStateForCheckpoint.get()) {
            final LogstoreShardHandle shardMeta = cursor.f0;
            final String checkpoint = cursor.f1;
            cursorsToRestore.put(shardMeta, checkpoint);
        }
        LOG.info("The following offsets restored from Flink state: {}", cursorsToRestore);
    }

    @Override
    public TypeInformation<SourceRecord> getProducedType() {
        return PojoTypeInfo.of(SourceRecord.class);
    }

    @Override
    public void notifyCheckpointComplete(long l) {
    }

    @Override
    public void close() throws Exception {
        cancel();
        super.close();
    }
}
