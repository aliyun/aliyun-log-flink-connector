package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.flink.model.*;
import com.aliyun.openservices.log.flink.util.*;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.http.signer.SignVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static com.aliyun.openservices.log.flink.ConfigConstants.REGION_ID;

public class FlinkLogConsumer<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>,
        CheckpointedFunction, CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogConsumer.class);
    private static final long serialVersionUID = 7835636734161627680L;

    private static final String CURSOR_STATE_STORE_NAME = "LogStore-Shard-State";

    private final Properties configProps;
    private final LogDeserializationSchema<T> deserializer;
    private transient LogDataFetcher<T> fetcher;
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
    private final MemoryLimiter memoryLimiter;

    @Deprecated
    public FlinkLogConsumer(LogDeserializationSchema<T> deserializer, Properties configProps) {
        this.configProps = configProps;
        this.deserializer = deserializer;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = configProps.getProperty(ConfigConstants.LOG_PROJECT);
        this.logstores = Collections.singletonList(configProps.getProperty(ConfigConstants.LOG_LOGSTORE));
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
        this.memoryLimiter = new MemoryLimiter(configProps);
    }

    public FlinkLogConsumer(String project, List<String> logstores, LogDeserializationSchema<T> deserializer, Properties configProps) {
        if (project == null || project.isEmpty()) {
            throw new IllegalArgumentException("The project is null or empty");
        }
        if (logstores == null || logstores.isEmpty()) {
            throw new IllegalArgumentException("The logstores is null or empty");
        }
        this.configProps = configProps;
        this.deserializer = deserializer;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = project;
        this.logstores = logstores;
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
        this.memoryLimiter = new MemoryLimiter(configProps);
    }

    public FlinkLogConsumer(String project, String logstore, LogDeserializationSchema<T> deserializer, Properties configProps) {
        this(project, Collections.singletonList(logstore), deserializer, configProps);
        if (logstore == null || logstore.isEmpty()) {
            throw new IllegalArgumentException("The logstore is null or empty");
        }
    }

    public FlinkLogConsumer(String project, Pattern logstorePattern, LogDeserializationSchema<T> deserializer, Properties configProps) {
        if (project == null || project.isEmpty()) {
            throw new IllegalArgumentException("The project is null or empty");
        }
        if (logstorePattern == null) {
            throw new IllegalArgumentException("The logstore pattern is null");
        }
        this.configProps = configProps;
        this.deserializer = deserializer;
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        this.project = project;
        this.logstorePattern = logstorePattern;
        this.checkpointMode = LogUtil.parseCheckpointMode(configProps);
        this.memoryLimiter = new MemoryLimiter(configProps);
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
            String regionId = parser.getString(REGION_ID);
            if (StringUtils.isBlank(regionId)) {
                throw new IllegalArgumentException("The " + REGION_ID + " was not specified for signature " + signVersion.name() + ".");
            }
            clientConfig.setRegion(regionId);
        }
        clientConfig.setSignatureVersion(signVersion);
        String processor = parser.getString(ConfigConstants.PROCESSOR);
        logClient = new LogClientProxy(
                parser.getString(ConfigConstants.LOG_ENDPOINT),
                parser.getString(ConfigConstants.LOG_ACCESSKEYID),
                parser.getString(ConfigConstants.LOG_ACCESSKEY),
                getOrCreateUserAgent(indexOfSubTask),
                retryPolicy,
                memoryLimiter,
                clientConfig,
                processor);
    }

    public void setShardAssigner(ShardAssigner shardAssigner) {
        this.shardAssigner = shardAssigner;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        final RuntimeContext ctx = getRuntimeContext();
        createClientIfNeeded(ctx.getIndexOfThisSubtask());
        LogDataFetcher<T> fetcher = new LogDataFetcher<>(sourceContext, ctx, project,
                logstores, logstorePattern,
                configProps, deserializer,
                logClient, checkpointMode,
                shardAssigner, memoryLimiter);
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
        LogDataFetcher<T> fetcher = this.fetcher;
        if (fetcher != null) {
            try {
                // interrupt the fetcher of any work
                fetcher.shutdownFetcher();
            } catch (Exception e) {
                LOG.warn("Error while closing data fetcher", e);
            }
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
            updateCheckpoint(shardMeta, cursor);
        }
    }

    private void updateCheckpoint(LogstoreShardMeta meta, String cursor) throws Exception {
        logClient.updateCheckpoint(project, meta.getLogstore(),
                consumerGroup, meta.getShardId(), meta.isReadOnly(),
                cursor);
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
        for (Tuple2<LogstoreShardMeta, String> cursor : cursorStateForCheckpoint.get()) {
            final LogstoreShardMeta shardMeta = cursor.f0;
            final String checkpoint = cursor.f1;
            cursorsToRestore.put(shardMeta, checkpoint);
        }
        LOG.info("The following offsets restored from Flink state: {}", cursorsToRestore);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long l) {
    }

    @Override
    public void close() throws Exception {
        cancel();
        if (fetcher != null) {
            fetcher.awaitTermination();
            fetcher = null;
        }
        super.close();
    }
}
