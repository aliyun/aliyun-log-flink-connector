package com.aliyun.openservices.log.flink.source;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSplitAssigner;
import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSourceEnumState;
import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSourceEnumStateSerializer;
import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSourceEnumerator;
import com.aliyun.openservices.log.flink.source.enumerator.assigner.ModuloSplitAssigner;
import com.aliyun.openservices.log.flink.source.metrics.AliyunLogSourceReaderMetrics;
import com.aliyun.openservices.log.flink.source.reader.AliyunLogSourceReader;
import com.aliyun.openservices.log.flink.source.reader.AliyunLogSplitReader;
import com.aliyun.openservices.log.flink.source.reader.fetcher.AliyunLogSourceFetcherManager;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplitSerializer;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;
import java.util.function.Supplier;

/**
 * Flink Source connector for Aliyun Log Service using the new Source API.
 */
public class AliyunLogSource<T> implements Source<T, AliyunLogSourceSplit, AliyunLogSourceEnumState>, ResultTypeQueryable<T> {
    private static final long serialVersionUID = 1L;

    private final String project;
    private final String logstore;
    private final Properties configProps;
    private final AliyunLogDeserializationSchema<T> deserializer;
    private final AliyunLogSplitAssigner splitAssigner;
    private final String accessKeyId;
    private final String accessKey;

    public AliyunLogSource(
            String project,
            String logstore,
            AliyunLogDeserializationSchema<T> deserializer,
            Properties configProps,
            AliyunLogSplitAssigner splitAssigner,
            String accessKeyId,
            String accessKey) {
        this.project = project;
        this.logstore = logstore;
        this.deserializer = deserializer;
        this.configProps = configProps;
        this.splitAssigner = splitAssigner != null ? splitAssigner : new ModuloSplitAssigner();
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
    }

    /**
     * Create a {@link AliyunLogSourceBuilder} to allow the fluent construction of a new {@link
     * AliyunLogSource}.
     *
     * @param <T> type of records being read
     * @return {@link AliyunLogSourceBuilder}
     */
    public static <T> AliyunLogSourceBuilder<T> builder() {
        return new AliyunLogSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        // Unbounded source - continuously reads from log service
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<AliyunLogSourceSplit, AliyunLogSourceEnumState> createEnumerator(
            SplitEnumeratorContext<AliyunLogSourceSplit> enumContext) {
        // Delegate to restoreEnumerator with null checkpoint for fresh start
        return restoreEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<AliyunLogSourceSplit, AliyunLogSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<AliyunLogSourceSplit> enumContext,
            AliyunLogSourceEnumState checkpoint) {
        // Pass checkpoint to enumerator constructor to restore state
        return new AliyunLogSourceEnumerator(
                enumContext, project, logstore, accessKeyId, accessKey,
                configProps, splitAssigner, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<AliyunLogSourceSplit> getSplitSerializer() {
        return new AliyunLogSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<AliyunLogSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new AliyunLogSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<T, AliyunLogSourceSplit> createReader(SourceReaderContext readerContext) {
        LogClientProxy logClient = LogClientProxy.makeClient(configProps, accessKeyId, accessKey, readerContext.getIndexOfSubtask());
        String consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        AliyunLogSourceReaderMetrics sourceReaderMetrics =
                new AliyunLogSourceReaderMetrics(readerContext.metricGroup());
        // Create a supplier that creates a new split reader each time it's called
        // Each fetcher will get its own reader instance, and each reader handles one split
        Supplier<SplitReader<PullLogsResult, AliyunLogSourceSplit>> splitReaderSupplier =
                () -> new AliyunLogSplitReader(logClient, project, consumerGroup, configProps, sourceReaderMetrics);
        // Create the fetcher manager with one fetcher per split
        AliyunLogSourceFetcherManager fetcherManager = new AliyunLogSourceFetcherManager(
                splitReaderSupplier,
                new Configuration());

        return new AliyunLogSourceReader<>(
                readerContext, logClient, project, deserializer, configProps, fetcherManager);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }
}

