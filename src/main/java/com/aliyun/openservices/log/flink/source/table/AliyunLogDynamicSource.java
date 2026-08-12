package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.source.AliyunLogSource;
import com.aliyun.openservices.log.flink.source.AliyunLogSourceBuilder;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

/**
 * SQL/Table source for Aliyun Log Service.
 */
public class AliyunLogDynamicSource implements ScanTableSource {
    private final String project;
    private final String logstore;
    private final String endpoint;
    private final LogCredentialsProviderFactory credentialsProviderFactory;
    private final Properties properties;
    private final RowType rowType;
    private final boolean ignoreParseErrors;
    private final Integer sourceParallelism;

    public AliyunLogDynamicSource(
            String project,
            String logstore,
            String endpoint,
            String accessKeyId,
            String accessKey,
            Properties properties,
            RowType rowType,
            boolean ignoreParseErrors,
            Integer sourceParallelism) {
        this(project,
                logstore,
                endpoint,
                new StaticCredentialsProviderFactory(accessKeyId, accessKey),
                properties,
                rowType,
                ignoreParseErrors,
                sourceParallelism);
    }

    public AliyunLogDynamicSource(
            String project,
            String logstore,
            String endpoint,
            LogCredentialsProviderFactory credentialsProviderFactory,
            Properties properties,
            RowType rowType,
            boolean ignoreParseErrors,
            Integer sourceParallelism) {
        this.project = project;
        this.logstore = logstore;
        this.endpoint = endpoint;
        this.credentialsProviderFactory = credentialsProviderFactory;
        this.properties = properties;
        this.rowType = rowType;
        this.ignoreParseErrors = ignoreParseErrors;
        this.sourceParallelism = sourceParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AliyunLogRowDataDeserializationSchema deserializer =
                new AliyunLogRowDataDeserializationSchema(rowType, ignoreParseErrors);

        AliyunLogSourceBuilder<RowData> builder = AliyunLogSource.<RowData>builder()
                .setProject(project)
                .setLogStore(logstore)
                .setEndpoint(endpoint)
                .setCredentialsProviderFactory(credentialsProviderFactory)
                .setDeserializer(deserializer)
                .setProperties(properties);

        Source<RowData, ?, ?> source = builder.build();
        if (sourceParallelism == null) {
            return SourceProvider.of(source);
        }
        return SourceProvider.of(source, sourceParallelism);
    }

    @Override
    public DynamicTableSource copy() {
        return new AliyunLogDynamicSource(
                project,
                logstore,
                endpoint,
                credentialsProviderFactory,
                copyProperties(properties),
                rowType,
                ignoreParseErrors,
                sourceParallelism);
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
