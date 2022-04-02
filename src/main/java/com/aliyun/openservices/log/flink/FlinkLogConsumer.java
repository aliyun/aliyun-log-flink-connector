package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.internal.ConfigParser;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.model.LogDataFetcher;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.flink.model.SourceRecord;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.common.ExecutionConfig;
import com.aliyun.openservices.log.flink.util.RetryPolicy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
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
    private transient ListState<Tuple2<LogstoreShardMeta, String>> cursorStateForCheckpoint;
    private transient HashMap<LogstoreShardMeta, String> cursorsToRestore;
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

    private String getOrCreateUserAgent(int indexOfSubTask) {
        String userAgent = configProps.getProperty(ConfigConstants.LOG_USER_AGENT);
        if (userAgent != null && !userAgent.isEmpty()) {
            return userAgent;
        }
        userAgent = "Flink-Connector-" + Consts.FLINK_CONNECTOR_VERSION;
        if (consumerGroup != null) {
            userAgent += "-" + consumerGroup + "/" + indexOfSubTask;
        } else {
            userAgent += "/" + indexOfSubTask;
        }
        return userAgent;
    }

    private void createClientIfNeeded(int indexOfSubTask) {
        if (logClient != null) {
            return;
        }
        ConfigParser parser = new ConfigParser(configProps);
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(parser.getInt(ConfigConstants.MAX_RETRIES, Consts.DEFAULT_MAX_RETRIES))
                .maxRetriesForRetryableError(parser.getInt(ConfigConstants.MAX_RETRIES_FOR_RETRYABLE_ERROR,
                        Consts.DEFAULT_MAX_RETRIES_FOR_RETRYABLE_ERROR))
                .baseRetryBackoff(parser.getLong(ConfigConstants.BASE_RETRY_BACKOFF_TIME_MS,
                        Consts.DEFAULT_BASE_RETRY_BACKOFF_TIME_MS))
                .maxRetryBackoff(parser.getLong(ConfigConstants.MAX_RETRY_BACKOFF_TIME_MS,
                        Consts.DEFAULT_MAX_RETRY_BACKOFF_TIME_MS))
                .build();
        logClient = new LogClientProxy(
                parser.getString(ConfigConstants.LOG_ENDPOINT),
                parser.getString(ConfigConstants.LOG_ACCESSKEYID),
                parser.getString(ConfigConstants.LOG_ACCESSKEY),
                getOrCreateUserAgent(indexOfSubTask),
                retryPolicy);
        if (parser.getBool(ConfigConstants.DIRECT_MODE, false)) {
            logClient.enableDirectMode(project);
        }
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
     *
     * @param periodicWatermarkAssigner periodic watermark assigner
     */
    public void setPeriodicWatermarkAssigner(
            AssignerWithPeriodicWatermarks<SourceRecord> periodicWatermarkAssigner) {
        this.periodicWatermarkAssigner = periodicWatermarkAssigner;
        ClosureCleaner.clean(this.periodicWatermarkAssigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    @Override
    public void run(SourceContext<SourceRecord> sourceContext) throws Exception {
        final RuntimeContext ctx = getRuntimeContext();
        createClientIfNeeded(ctx.getIndexOfThisSubtask());
        LogDataFetcher fetcher = new LogDataFetcher(sourceContext,
                ctx, project,
                logstores, logstorePattern,
                configProps,
                logClient,
                checkpointMode,
                shardAssigner,
                periodicWatermarkAssigner);
        List<LogstoreShardMeta> newShards = fetcher.discoverNewShardsToSubscribe();
        for (LogstoreShardMeta shard : newShards) {
            String checkpoint = null;
            if (cursorsToRestore != null && cursorsToRestore.containsKey(shard)) {
                checkpoint = cursorsToRestore.get(shard);
            }
            fetcher.registerNewSubscribedShard(shard, checkpoint);
        }
        if (!running) {
            return;
        }
        this.fetcher = fetcher;
        fetcher.runFetcher();
        fetcher.awaitTermination();
        if (logClient != null) {
            logClient.close();
            logClient = null;
        }
        sourceContext.close();
    }

    @Override
    public void cancel() {
        running = false;
        if (fetcher != null) {
            fetcher.cancel();
            LOG.warn("Log fetcher has been canceled");
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
        final RuntimeContext ctx = getRuntimeContext();
        createClientIfNeeded(ctx.getIndexOfThisSubtask());
        if (fetcher == null) {
            if (cursorsToRestore == null)
                return;
            int numberOfParallelTasks = ctx.getNumberOfParallelSubtasks();
            int indexOfThisTask = ctx.getIndexOfThisSubtask();
            for (Map.Entry<LogstoreShardMeta, String> entry : cursorsToRestore.entrySet()) {
                // cursorsToRestore is the restored global union state;
                // should only snapshot shards that actually belong to us
                if (shardAssigner.assign(entry.getKey(), numberOfParallelTasks) % numberOfParallelTasks == indexOfThisTask) {
                    // Save to local state only. No need to sync with remote server
                    cursorStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
                }
            }
            return;
        }
        Map<LogstoreShardMeta, String> snapshotState = fetcher.snapshotState();
        if (LOG.isDebugEnabled()) {
            StringBuilder strb = new StringBuilder();
            for (Map.Entry<LogstoreShardMeta, String> entry : snapshotState.entrySet()) {
                strb.append("shard: ").append(entry.getKey().getShardId()).append(", cursor: ").append(entry.getValue());
            }
            LOG.debug("Snapshotted state, last processed cursor: {}, checkpoint id: {}, timestamp: {}",
                    strb, context.getCheckpointId(), context.getCheckpointTimestamp());
        }
        for (Map.Entry<LogstoreShardMeta, String> entry : snapshotState.entrySet()) {
            updateCursorState(entry.getKey(), entry.getValue());
        }
    }

    private void updateCursorState(LogstoreShardMeta shardMeta, String cursor) throws Exception {
        cursorStateForCheckpoint.add(Tuple2.of(shardMeta, cursor));
        if (cursor != null && consumerGroup != null && checkpointMode == CheckpointMode.ON_CHECKPOINTS) {
            logClient.updateCheckpoint(project,
                    shardMeta.getLogstore(),
                    consumerGroup,
                    shardMeta.getShardId(),
                    shardMeta.isReadOnly(),
                    cursor);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("Initializing state from Flink state");

        TypeInformation<Tuple2<LogstoreShardMeta, String>> shardsStateTypeInfo = new TupleTypeInfo<Tuple2<LogstoreShardMeta, String>>(
                TypeInformation.of(LogstoreShardMeta.class),
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
        RuntimeContext ctx = getRuntimeContext();
        createClientIfNeeded(ctx.getIndexOfThisSubtask());
        cursorsToRestore = new HashMap<>();
        for (Tuple2<LogstoreShardMeta, String> offset : cursorStateForCheckpoint.get()) {
            cursorsToRestore.put(offset.f0, offset.f1);
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
