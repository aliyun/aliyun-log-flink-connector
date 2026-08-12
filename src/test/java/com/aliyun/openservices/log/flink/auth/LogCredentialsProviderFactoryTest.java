package com.aliyun.openservices.log.flink.auth;

import com.aliyun.openservices.log.common.auth.Credentials;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.FlinkLogProducer;
import com.aliyun.openservices.log.flink.FlinkLogProducerV2;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.sink.AliyunLogSink;
import com.aliyun.openservices.log.flink.source.AliyunLogSource;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LogCredentialsProviderFactoryTest {

    @After
    public void resetCounter() {
        CountingCredentialsProviderFactory.CREATED.set(0);
    }

    @Test
    public void testStaticCredentialsProviderFactorySupportsSecurityToken() {
        Credentials credentials = new StaticCredentialsProviderFactory("id", "secret", "token")
                .createCredentialsProvider()
                .getCredentials();

        assertEquals("id", credentials.getAccessKeyId());
        assertEquals("secret", credentials.getAccessKeySecret());
        assertEquals("token", credentials.getSecurityToken());
    }

    @Test
    public void testReflectiveFactoryConfiguresApplicationFactoryAfterSerialization()
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty("roleArn", "test-role");
        ReflectiveLogCredentialsProviderFactory original =
                new ReflectiveLogCredentialsProviderFactory(
                        ConfigurableTestFactory.class.getName(),
                        properties);

        ReflectiveLogCredentialsProviderFactory restored = roundTrip(original);
        Credentials credentials = restored.createCredentialsProvider().getCredentials();

        assertEquals("test-role", credentials.getAccessKeyId());
        assertEquals("dynamic-secret", credentials.getAccessKeySecret());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReflectiveFactoryRejectsParametersForNonConfigurableFactory() {
        Properties properties = new Properties();
        properties.setProperty("roleArn", "test-role");
        new ReflectiveLogCredentialsProviderFactory(
                CountingCredentialsProviderFactory.class.getName(),
                properties)
                .createCredentialsProvider();
    }

    @Test
    public void testSourceAndSinkDoNotCreateProviderWhileBuildingOrSerializing()
            throws Exception {
        CountingCredentialsProviderFactory factory = new CountingCredentialsProviderFactory();
        AliyunLogSource<String> source = AliyunLogSource.<String>builder()
                .setProject("project")
                .setLogStore("logstore")
                .setEndpoint("cn-hangzhou.log.aliyuncs.com")
                .setCredentialsProviderFactory(factory)
                .setDeserializer(new StringDeserializer())
                .build();
        AliyunLogSink<String> sink = AliyunLogSink.<String>builder()
                .setProject("project")
                .setLogStore("logstore")
                .setEndpoint("cn-hangzhou.log.aliyuncs.com")
                .setCredentialsProviderFactory(factory)
                .setSerializer((element, output) -> { })
                .build();

        assertNotNull(roundTrip(source));
        assertNotNull(roundTrip(sink));
        assertEquals(0, CountingCredentialsProviderFactory.CREATED.get());
    }

    @Test
    public void testDynamicCredentialModeRemovesStaticCredentialsFromSerializedConnectors()
            throws Exception {
        Properties legacyProperties = new Properties();
        legacyProperties.setProperty(ConfigConstants.LOG_ACCESSKEYID, "legacy-access-key-id");
        legacyProperties.setProperty(ConfigConstants.LOG_ACCESSKEY, "legacy-access-key-secret");
        CountingCredentialsProviderFactory factory = new CountingCredentialsProviderFactory();

        AliyunLogSource<String> source = AliyunLogSource.<String>builder()
                .setProject("project")
                .setLogStore("logstore")
                .setEndpoint("cn-hangzhou.log.aliyuncs.com")
                .setCredentialsProviderFactory(factory)
                .setProperties(legacyProperties)
                .setDeserializer(new StringDeserializer())
                .build();
        AliyunLogSink<String> sink = AliyunLogSink.<String>builder()
                .setProject("project")
                .setLogStore("logstore")
                .setEndpoint("cn-hangzhou.log.aliyuncs.com")
                .setCredentialsProviderFactory(factory)
                .setProperties(legacyProperties)
                .setSerializer((element, output) -> { })
                .build();

        assertSerializedFormDoesNotContainLegacyCredentials(source);
        assertSerializedFormDoesNotContainLegacyCredentials(sink);
    }

    @Test
    public void testLegacyConnectorsRemoveStaticCredentialsWhenFactoryIsConfigured()
            throws Exception {
        Properties sharedProperties = legacyProperties();
        CountingCredentialsProviderFactory factory = new CountingCredentialsProviderFactory();
        FlinkLogConsumer<RawLogGroupList> staticConsumer = new FlinkLogConsumer<>(
                new RawLogGroupListDeserializer(),
                sharedProperties);
        FlinkLogProducer<String> staticProducer = new FlinkLogProducer<>(
                value -> new RawLogGroup(),
                sharedProperties);
        FlinkLogProducerV2<String> staticProducerV2 = new FlinkLogProducerV2<>(
                (value, output) -> { },
                sharedProperties);
        FlinkLogConsumer<RawLogGroupList> consumer = new FlinkLogConsumer<>(
                new RawLogGroupListDeserializer(),
                sharedProperties)
                .setCredentialsProviderFactory(factory);
        FlinkLogProducer<String> producer = new FlinkLogProducer<String>(
                value -> new RawLogGroup(),
                sharedProperties)
                .setCredentialsProviderFactory(factory);
        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<String>(
                (value, output) -> { },
                sharedProperties)
                .setCredentialsProviderFactory(factory);

        assertEquals("legacy-access-key-id",
                sharedProperties.getProperty(ConfigConstants.LOG_ACCESSKEYID));
        assertEquals("legacy-access-key-secret",
                sharedProperties.getProperty(ConfigConstants.LOG_ACCESSKEY));
        assertSerializedFormContainsLegacyCredentials(staticConsumer);
        assertSerializedFormContainsLegacyCredentials(staticProducer);
        assertSerializedFormContainsLegacyCredentials(staticProducerV2);
        assertSerializedFormDoesNotContainLegacyCredentials(consumer);
        assertSerializedFormDoesNotContainLegacyCredentials(producer);
        assertSerializedFormDoesNotContainLegacyCredentials(producerV2);
    }

    @Test
    public void testDynamicSourceConstructorCopiesAndRemovesStaticCredentials()
            throws Exception {
        Properties properties = legacyProperties();
        AliyunLogSource<String> source = new AliyunLogSource<>(
                "project",
                "logstore",
                new StringDeserializer(),
                properties,
                null,
                new CountingCredentialsProviderFactory());

        assertEquals("legacy-access-key-id",
                properties.getProperty(ConfigConstants.LOG_ACCESSKEYID));
        assertEquals("legacy-access-key-secret",
                properties.getProperty(ConfigConstants.LOG_ACCESSKEY));
        assertSerializedFormDoesNotContainLegacyCredentials(source);
    }

    @Test
    public void testLogClientProxyCreatesProviderAtRuntime() {
        Properties properties = new Properties();
        properties.setProperty(
                ConfigConstants.LOG_ENDPOINT,
                "cn-hangzhou.log.aliyuncs.com");

        LogClientProxy client = LogClientProxy.makeClient(
                properties,
                new CountingCredentialsProviderFactory(),
                0);
        try {
            assertEquals(1, CountingCredentialsProviderFactory.CREATED.get());
        } finally {
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Serializable> T roundTrip(T value) throws Exception {
        byte[] serialized = serialize(value);
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(serialized))) {
            return (T) input.readObject();
        }
    }

    private static byte[] serialize(Serializable value) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(value);
        }
        return buffer.toByteArray();
    }

    private static void assertSerializedFormDoesNotContainLegacyCredentials(Serializable value)
            throws Exception {
        String serialized = new String(serialize(value), StandardCharsets.ISO_8859_1);
        assertFalse(serialized.contains("legacy-access-key-id"));
        assertFalse(serialized.contains("legacy-access-key-secret"));
    }

    private static void assertSerializedFormContainsLegacyCredentials(Serializable value)
            throws Exception {
        String serialized = new String(serialize(value), StandardCharsets.ISO_8859_1);
        assertTrue(serialized.contains("legacy-access-key-id"));
        assertTrue(serialized.contains("legacy-access-key-secret"));
    }

    private static Properties legacyProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com");
        properties.setProperty(ConfigConstants.LOG_PROJECT, "project");
        properties.setProperty(ConfigConstants.LOG_LOGSTORE, "logstore");
        properties.setProperty(ConfigConstants.LOG_ACCESSKEYID, "legacy-access-key-id");
        properties.setProperty(ConfigConstants.LOG_ACCESSKEY, "legacy-access-key-secret");
        return properties;
    }

    public static class ConfigurableTestFactory
            implements ConfigurableLogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;

        private String roleArn;

        public ConfigurableTestFactory() {
        }

        @Override
        public void configure(Properties properties) {
            roleArn = properties.getProperty("roleArn");
        }

        @Override
        public CredentialsProvider createCredentialsProvider() {
            return new StaticCredentialsProvider(
                    new DefaultCredentials(roleArn, "dynamic-secret"));
        }
    }

    public static class CountingCredentialsProviderFactory
            implements LogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger CREATED = new AtomicInteger();

        public CountingCredentialsProviderFactory() {
        }

        @Override
        public CredentialsProvider createCredentialsProvider() {
            CREATED.incrementAndGet();
            return new StaticCredentialsProvider(
                    new DefaultCredentials("dynamic-id", "dynamic-secret"));
        }
    }

    private static class StringDeserializer
            implements AliyunLogDeserializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(PullLogsResult record, Collector<String> out) {
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
