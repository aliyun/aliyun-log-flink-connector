package com.aliyun.openservices.log.flink.sink;

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
    private final String accessKeyId;
    private final String accessKey;
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
        this.project = project;
        this.logstore = logstore;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
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
                accessKeyId,
                accessKey,
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
