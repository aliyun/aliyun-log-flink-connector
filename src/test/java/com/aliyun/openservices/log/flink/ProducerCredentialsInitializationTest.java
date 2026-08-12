package com.aliyun.openservices.log.flink;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProducerCredentialsInitializationTest {

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
