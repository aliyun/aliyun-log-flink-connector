package com.aliyun.openservices.log.flink.sink;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.AliyunLogSerializationSchema;

import java.util.Properties;

/**
 * Builder for {@link AliyunLogSink}.
 */
public class AliyunLogSinkBuilder<T> {
    private String project;
    private String logstore;
    private String endpoint;
    private String accessKeyId;
    private String accessKey;
    private Properties properties = new Properties();
    private AliyunLogSerializationSchema<T> serializer;

    public AliyunLogSinkBuilder<T> setProject(String project) {
        this.project = project;
        return this;
    }

    public AliyunLogSinkBuilder<T> setLogStore(String logstore) {
        this.logstore = logstore;
        return this;
    }

    public AliyunLogSinkBuilder<T> setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public AliyunLogSinkBuilder<T> setCredentials(String accessKeyId, String accessKey) {
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        return this;
    }

    public AliyunLogSinkBuilder<T> setSerializer(AliyunLogSerializationSchema<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    public AliyunLogSinkBuilder<T> setProperty(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }

    public AliyunLogSinkBuilder<T> setProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public AliyunLogSink<T> build() {
        validateRequired("project", project);
        validateRequired("logstore", logstore);
        validateRequired("endpoint", endpoint);
        validateRequired("accessKeyId", accessKeyId);
        validateRequired("accessKey", accessKey);
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must be set");
        }

        Properties copied = new Properties();
        copied.putAll(properties);
        copied.setProperty(ConfigConstants.LOG_PROJECT, project);
        copied.setProperty(ConfigConstants.LOG_LOGSTORE, logstore);
        copied.setProperty(ConfigConstants.LOG_ENDPOINT, endpoint);
        copied.setProperty(ConfigConstants.LOG_ACCESSKEYID, accessKeyId);
        copied.setProperty(ConfigConstants.LOG_ACCESSKEY, accessKey);

        return new AliyunLogSink<>(
                project,
                logstore,
                endpoint,
                accessKeyId,
                accessKey,
                copied,
                serializer);
    }

    private static void validateRequired(String name, String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must be set");
        }
    }
}
