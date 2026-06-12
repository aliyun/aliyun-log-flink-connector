package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.flink.sink.AliyunLogSink;
import com.aliyun.openservices.log.flink.sink.AliyunLogSinkBuilder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

/**
 * SQL/Table sink for Aliyun Log Service.
 */
public class AliyunLogDynamicSink implements DynamicTableSink {
    private final String project;
    private final String logstore;
    private final String endpoint;
    private final String accessKeyId;
    private final String accessKey;
    private final Properties properties;
    private final RowType rowType;
    private final String defaultTopic;
    private final String defaultSource;
    private final Integer sinkParallelism;

    public AliyunLogDynamicSink(
            String project,
            String logstore,
            String endpoint,
            String accessKeyId,
            String accessKey,
            Properties properties,
            RowType rowType,
            String defaultTopic,
            String defaultSource,
            Integer sinkParallelism) {
        this.project = project;
        this.logstore = logstore;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        this.properties = properties;
        this.rowType = rowType;
        this.defaultTopic = defaultTopic;
        this.defaultSource = defaultSource;
        this.sinkParallelism = sinkParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AliyunLogRowDataSerializationSchema serializer =
                new AliyunLogRowDataSerializationSchema(rowType, defaultTopic, defaultSource);
        AliyunLogSinkBuilder<RowData> builder = AliyunLogSink.<RowData>builder()
                .setProject(project)
                .setLogStore(logstore)
                .setEndpoint(endpoint)
                .setCredentials(accessKeyId, accessKey)
                .setSerializer(serializer)
                .setProperties(properties);
        Sink<RowData> sink = builder.build();
        if (sinkParallelism == null) {
            return SinkV2Provider.of(sink);
        }
        return SinkV2Provider.of(sink, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new AliyunLogDynamicSink(
                project,
                logstore,
                endpoint,
                accessKeyId,
                accessKey,
                copyProperties(properties),
                rowType,
                defaultTopic,
                defaultSource,
                sinkParallelism);
    }

    @Override
    public String asSummaryString() {
        return "AliyunLog";
    }

    private static Properties copyProperties(Properties source) {
        Properties copy = new Properties();
        copy.putAll(source);
        return copy;
    }
}
