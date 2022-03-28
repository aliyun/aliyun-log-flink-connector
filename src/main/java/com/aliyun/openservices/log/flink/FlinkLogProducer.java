package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.TagContent;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.internal.ConfigParser;
import com.aliyun.openservices.log.flink.internal.Producer;
import com.aliyun.openservices.log.flink.internal.ProducerConfig;
import com.aliyun.openservices.log.flink.internal.ProducerImpl;
import com.aliyun.openservices.log.flink.model.LogSerializationSchema;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.RetryPolicy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.aliyun.openservices.log.flink.ConfigConstants.FLUSH_INTERVAL_MS;
import static com.aliyun.openservices.log.flink.ConfigConstants.IO_THREAD_NUM;

public class FlinkLogProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLogProducer.class);
    private final LogSerializationSchema<T> schema;
    private LogPartitioner<T> customPartitioner = null;
    private transient Producer producer;
    private final Properties properties;

    public FlinkLogProducer(final LogSerializationSchema<T> schema, Properties configProps) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (configProps == null) {
            throw new IllegalArgumentException("configProps cannot be null");
        }
        this.schema = schema;
        this.properties = configProps;
    }

    public void setCustomPartitioner(LogPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    private Producer createProducer(ConfigParser parser) {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setFlushInterval(parser.getLong(FLUSH_INTERVAL_MS,
                ProducerConfig.DEFAULT_LINGER_MS));
        producerConfig.setIoThreadNum(
                parser.getInt(IO_THREAD_NUM, ProducerConfig.DEFAULT_IO_THREAD_COUNT));
        producerConfig.setProject(parser.getString(ConfigConstants.LOG_PROJECT));
        producerConfig.setLogstore(parser.getString(ConfigConstants.LOG_LOGSTORE));
        producerConfig.setEndpoint(parser.getString(ConfigConstants.LOG_ENDPOINT));
        producerConfig.setAccessKeyId(parser.getString(ConfigConstants.LOG_ACCESSKEYID));
        producerConfig.setAccessKeySecret(parser.getString(ConfigConstants.LOG_ACCESSKEY));
        producerConfig.setTotalSizeInBytes(parser.getInt(ConfigConstants.TOTAL_SIZE_IN_BYTES,
                ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES));
        producerConfig.setLogGroupSize(parser.getInt(ConfigConstants.LOG_GROUP_MAX_SIZE,
                ProducerConfig.DEFAULT_LOG_GROUP_SIZE));
        producerConfig.setLogGroupMaxLines(parser.getInt(ConfigConstants.LOG_GROUP_MAX_LINES,
                ProducerConfig.DEFAULT_MAX_LOG_GROUP_LINES));
        producerConfig.setProducerQueueSize(parser.getInt(ConfigConstants.PRODUCER_QUEUE_SIZE,
                ProducerConfig.DEFAULT_PRODUCER_QUEUE_SIZE));
        producerConfig.setBuckets(parser.getInt(ConfigConstants.BUCKETS,
                ProducerConfig.DEFAULT_BUCKETS));
        producerConfig.setAdjustShardHash(parser.getBool(ConfigConstants.PRODUCER_ADJUST_SHARD_HASH,
                ProducerConfig.DEFAULT_ADJUST_SHARD_HASH));
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(parser.getInt(ConfigConstants.MAX_RETRIES, Consts.DEFAULT_MAX_RETRIES))
                .maxRetriesForRetryableError(parser.getInt(ConfigConstants.MAX_RETRIES_FOR_RETRYABLE_ERROR,
                        Consts.DEFAULT_MAX_RETRIES_FOR_RETRYABLE_ERROR))
                .baseRetryBackoff(parser.getLong(ConfigConstants.BASE_RETRY_BACKOFF_TIME_MS,
                        Consts.DEFAULT_BASE_RETRY_BACKOFF_TIME_MS))
                .maxRetryBackoff(parser.getLong(ConfigConstants.MAX_RETRY_BACKOFF_TIME_MS,
                        Consts.DEFAULT_MAX_RETRY_BACKOFF_TIME_MS))
                .build();
        return new ProducerImpl(producerConfig, retryPolicy);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (customPartitioner != null) {
            customPartitioner.initialize(
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks());
        }
        if (producer == null) {
            ConfigParser parser = new ConfigParser(properties);
            producer = createProducer(parser);
            producer.open();
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (producer != null) {
            try {
                producer.flush();
            } catch (InterruptedException ex) {
                LOG.warn("Interrupted while flushing producer.");
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

    @Override
    public void invoke(T value, Context context) {
        if (this.producer == null) {
            throw new IllegalStateException("Flink log producer has not been initialized yet!");
        }
        RawLogGroup logGroup = schema.serialize(value);
        if (logGroup == null) {
            LOG.info("Skip null LogGroup");
            return;
        }
        String shardHashKey = null;
        if (customPartitioner != null) {
            shardHashKey = customPartitioner.getHashKey(value);
        }
        List<LogItem> logs = new ArrayList<>();
        for (RawLog rawLog : logGroup.getLogs()) {
            if (rawLog == null) {
                continue;
            }
            LogItem record = new LogItem(rawLog.getTime());
            for (Map.Entry<String, String> kv : rawLog.getContents().entrySet()) {
                String key = kv.getKey();
                if (key == null) {
                    continue;
                }
                record.PushBack(key, kv.getValue());
            }
            logs.add(record);
        }
        if (logs.isEmpty()) {
            return;
        }
        List<TagContent> tags = getTags(logGroup);
        try {
            producer.send(logGroup.getTopic(), logGroup.getSource(), shardHashKey, tags, logs);
        } catch (InterruptedException e) {
            LOG.error("Error while sending logs", e);
            throw new RuntimeException(e);
        }
    }

    private static List<TagContent> getTags(RawLogGroup logGroup) {
        final Map<String, String> tags = logGroup.getTags();
        if (tags == null || tags.isEmpty()) {
            return Collections.emptyList();
        }
        List<TagContent> tagContents = new ArrayList<>();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            if (tag.getKey() != null) {
                tagContents.add(new TagContent(tag.getKey(), tag.getValue()));
            }
        }
        return tagContents;
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        super.close();
        LOG.info("Flink log producer has been closed");
    }
}
