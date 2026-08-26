package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.auth.Credentials;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.util.ProducerFactory;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProducerCredentialsInitializationTest {

    @Test
    public void testInvalidProducerConfigDoesNotCreateCredentialsProvider() {
        Properties properties = producerProperties();
        properties.setProperty(ConfigConstants.SIGNATURE_VERSION, "v4");
        AtomicInteger providerCreations = new AtomicInteger();
        LogCredentialsProviderFactory factory = () -> {
            providerCreations.incrementAndGet();
            return new StaticCredentialsProvider(
                    new DefaultCredentials("id", "secret"));
        };

        try {
            ProducerFactory.create(
                    "project",
                    properties.getProperty(ConfigConstants.LOG_ENDPOINT),
                    properties,
                    factory);
            fail("Expected missing region.id to fail producer configuration validation");
        } catch (IllegalArgumentException expected) {
            assertEquals(0, providerCreations.get());
        }
    }

    @Test
    public void testLegacyProducersDoNotStartThreadsWhenCredentialsFactoryFails()
            throws Exception {
        Properties properties = producerProperties();
        LogCredentialsProviderFactory failingFactory = new FailingCredentialsProviderFactory();
        Set<String> threadsBefore = producerThreadNames();

        FlinkLogProducer<String> producer = new FlinkLogProducer<String>(
                value -> new RawLogGroup(),
                properties)
                .setCredentialsProviderFactory(failingFactory);
        assertOpenFails(producer, new Configuration());

        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<String>(
                (value, collector) -> { },
                properties)
                .setCredentialsProviderFactory(failingFactory);
        assertOpenFails(producerV2, new Configuration());

        assertEquals(threadsBefore, producerThreadNames());
    }

    @Test
    public void testLegacyProducersKeepDefaultProducerUserAgent() throws Exception {
        LogCredentialsProviderFactory factory =
                new StaticCredentialsProviderFactory("id", "secret");
        FlinkLogProducer<String> producer = new FlinkLogProducer<String>(
                value -> new RawLogGroup(),
                producerProperties())
                .setCredentialsProviderFactory(factory);
        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<String>(
                (value, collector) -> { },
                producerProperties())
                .setCredentialsProviderFactory(factory);

        try {
            producer.open(new Configuration());
            producerV2.open(new Configuration());
            assertDefaultProducerUserAgent(getProducer(producer));
            assertDefaultProducerUserAgent(getProducer(producerV2));
        } finally {
            producer.close();
            producerV2.close();
        }
    }

    @Test
    public void testStaticLegacyProducersKeepDefaultProducerUserAgent() throws Exception {
        Properties properties = producerProperties();
        properties.setProperty(ConfigConstants.LOG_ACCESSKEYID, "id");
        properties.setProperty(ConfigConstants.LOG_ACCESSKEY, "secret");
        FlinkLogProducer<String> producer = new FlinkLogProducer<>(
                value -> new RawLogGroup(),
                properties);
        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<>(
                (value, collector) -> { },
                properties);

        try {
            producer.open(new Configuration());
            producerV2.open(new Configuration());
            assertDefaultProducerUserAgent(getProducer(producer));
            assertDefaultProducerUserAgent(getProducer(producerV2));
        } finally {
            producer.close();
            producerV2.close();
        }
    }

    @Test
    public void testStaticLegacyProducersResolveCredentialsWhenOpened() throws Exception {
        Properties properties = producerProperties();
        FlinkLogProducer<String> producer = new FlinkLogProducer<>(
                value -> new RawLogGroup(),
                properties);
        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<>(
                (value, collector) -> { },
                properties);
        properties.setProperty(ConfigConstants.LOG_ACCESSKEYID, "late-id");
        properties.setProperty(ConfigConstants.LOG_ACCESSKEY, "late-secret");

        try {
            producer.open(new Configuration());
            producerV2.open(new Configuration());
            assertProducerCredentials(getProducer(producer), "late-id", "late-secret");
            assertProducerCredentials(getProducer(producerV2), "late-id", "late-secret");
        } finally {
            producer.close();
            producerV2.close();
        }
    }

    @Test
    public void testStaticLegacyProducersKeepMissingCredentialFailureAtOpen()
            throws Exception {
        FlinkLogProducer<String> producer = new FlinkLogProducer<>(
                value -> new RawLogGroup(),
                producerProperties());
        FlinkLogProducerV2<String> producerV2 = new FlinkLogProducerV2<>(
                (value, collector) -> { },
                producerProperties());

        assertStaticOpenFails(producer, new Configuration());
        assertStaticOpenFails(producerV2, new Configuration());
    }

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.LOG_PROJECT, "project");
        properties.setProperty(ConfigConstants.LOG_LOGSTORE, "logstore");
        properties.setProperty(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com");
        return properties;
    }

    private static void assertOpenFails(FlinkLogProducer<String> producer, Configuration config)
            throws Exception {
        try {
            producer.open(config);
            fail("Expected credentials provider creation to fail");
        } catch (CredentialsProviderCreationException expected) {
            // Expected before LogProducer starts its background threads.
        }
    }

    private static void assertOpenFails(FlinkLogProducerV2<String> producer, Configuration config)
            throws Exception {
        try {
            producer.open(config);
            fail("Expected credentials provider creation to fail");
        } catch (CredentialsProviderCreationException expected) {
            // Expected before LogProducer starts its background threads.
        }
    }

    private static Set<String> producerThreadNames() {
        Set<String> names = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith("aliyun-log-producer-")) {
                names.add(thread.getName());
            }
        }
        return names;
    }

    private static Producer getProducer(Object connector) throws Exception {
        Field producerField = connector.getClass().getDeclaredField("producer");
        producerField.setAccessible(true);
        return (Producer) producerField.get(connector);
    }

    private static void assertProducerCredentials(
            Producer producer,
            String expectedAccessKeyId,
            String expectedAccessKeySecret) throws Exception {
        Field credentialsProviderField = Client.class.getDeclaredField("credentialsProvider");
        credentialsProviderField.setAccessible(true);
        CredentialsProvider credentialsProvider = (CredentialsProvider) credentialsProviderField.get(
                getClient(producer));
        Credentials credentials = credentialsProvider.getCredentials();
        assertEquals(expectedAccessKeyId, credentials.getAccessKeyId());
        assertEquals(expectedAccessKeySecret, credentials.getAccessKeySecret());
    }

    private static void assertStaticOpenFails(
            FlinkLogProducer<String> producer,
            Configuration config) throws Exception {
        try {
            producer.open(config);
            fail("Expected missing static credentials to fail at open");
        } catch (NullPointerException expected) {
            // Matches the static ProjectConfig validation in 0.1.46.
        }
    }

    private static void assertStaticOpenFails(
            FlinkLogProducerV2<String> producer,
            Configuration config) throws Exception {
        try {
            producer.open(config);
            fail("Expected missing static credentials to fail at open");
        } catch (NullPointerException expected) {
            // Matches the static ProjectConfig validation in 0.1.46.
        }
    }

    public static void assertDefaultProducerUserAgent(Producer producer) throws Exception {
        assertEquals(ProjectConfig.DEFAULT_USER_AGENT,
                getClient(producer).getUserAgent());
    }

    @SuppressWarnings("unchecked")
    private static Client getClient(Producer producer) throws Exception {
        Field clientPoolField = LogProducer.class.getDeclaredField("clientPool");
        clientPoolField.setAccessible(true);
        Map<String, Client> clientPool =
                (Map<String, Client>) clientPoolField.get(producer);
        return clientPool.get("project");
    }

    private static final class FailingCredentialsProviderFactory
            implements LogCredentialsProviderFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public CredentialsProvider createCredentialsProvider() {
            throw new CredentialsProviderCreationException();
        }
    }

    private static final class CredentialsProviderCreationException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
