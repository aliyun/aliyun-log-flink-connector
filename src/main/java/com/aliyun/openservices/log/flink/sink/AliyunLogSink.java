package com.aliyun.openservices.log.flink.sink;

import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.model.AliyunLogSerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.Properties;

/**
 * FLIP-style Sink API implementation for Aliyun Log Service.
 *
 * <p>The sink flushes all in-flight producer requests on Flink checkpoints and
 * therefore provides at-least-once delivery. SLS Producer does not expose a
 * transaction protocol that can be coordinated by Flink, so this sink does not
 * claim exactly-once semantics.
 */
public class AliyunLogSink<T> implements Sink<T> {
    private final String project;
    private final String logstore;
    private final String endpoint;
    private final LogCredentialsProviderFactory credentialsProviderFactory;
    private final Properties properties;
    private final AliyunLogSerializationSchema<T> schema;

    AliyunLogSink(
            String project,
            String logstore,
            String endpoint,
            String accessKeyId,
            String accessKey,
            Properties properties,
            AliyunLogSerializationSchema<T> schema) {
        this(project,
                logstore,
                endpoint,
                new StaticCredentialsProviderFactory(accessKeyId, accessKey),
                properties,
                schema);
    }

    AliyunLogSink(
            String project,
            String logstore,
            String endpoint,
            LogCredentialsProviderFactory credentialsProviderFactory,
            Properties properties,
            AliyunLogSerializationSchema<T> schema) {
        this.project = project;
        this.logstore = logstore;
        this.endpoint = endpoint;
        if (credentialsProviderFactory == null) {
            throw new IllegalArgumentException("CredentialsProviderFactory must not be null");
        }
        this.credentialsProviderFactory = credentialsProviderFactory;
        this.properties = copyProperties(properties);
        this.schema = schema;
    }

    public static <T> AliyunLogSinkBuilder<T> builder() {
        return new AliyunLogSinkBuilder<>();
    }

    public static <T> AliyunLogSinkBuilder<T> builder(AliyunLogSerializationSchema<T> schema) {
        return AliyunLogSink.<T>builder().setSerializer(schema);
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new AliyunLogSinkWriter<>(
                project,
                logstore,
                endpoint,
                credentialsProviderFactory,
                copyProperties(properties),
                schema);
    }

    private static Properties copyProperties(Properties source) {
        Properties copy = new Properties();
        if (source != null) {
            copy.putAll(source);
        }
        return copy;
    }
}
