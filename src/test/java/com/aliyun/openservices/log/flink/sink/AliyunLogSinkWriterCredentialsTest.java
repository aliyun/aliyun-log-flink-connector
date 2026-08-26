package com.aliyun.openservices.log.flink.sink;

import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.flink.ProducerCredentialsInitializationTest;
import com.aliyun.openservices.log.flink.auth.LogCredentialsProviderFactory;
import com.aliyun.openservices.log.flink.auth.StaticCredentialsProviderFactory;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AliyunLogSinkWriterCredentialsTest {

    @Test
    public void testSinkWriterDoesNotStartThreadsWhenCredentialsFactoryFails() {
        Set<String> threadsBefore = producerThreadNames();

        try {
            new AliyunLogSinkWriter<>(
                    "project",
                    "logstore",
                    "cn-hangzhou.log.aliyuncs.com",
                    new FailingCredentialsProviderFactory(),
                    new Properties(),
                    (element, output) -> { });
            fail("Expected credentials provider creation to fail");
        } catch (CredentialsProviderCreationException expected) {
            // Expected before LogProducer starts its background threads.
        }

        assertEquals(threadsBefore, producerThreadNames());
    }

    @Test
    public void testSinkWriterKeepsDefaultProducerUserAgent() throws Exception {
        AliyunLogSinkWriter<String> writer = new AliyunLogSinkWriter<>(
                "project",
                "logstore",
                "cn-hangzhou.log.aliyuncs.com",
                new StaticCredentialsProviderFactory("id", "secret"),
                new Properties(),
                (element, output) -> { });
        try {
            Field producerField = AliyunLogSinkWriter.class.getDeclaredField("producer");
            producerField.setAccessible(true);
            ProducerCredentialsInitializationTest.assertDefaultProducerUserAgent(
                    (Producer) producerField.get(writer));
        } finally {
            writer.close();
        }
    }

    @Test
    public void testStaticSinkWriterKeepsDefaultProducerUserAgent() throws Exception {
        AliyunLogSinkWriter<String> writer = new AliyunLogSinkWriter<>(
                "project",
                "logstore",
                "cn-hangzhou.log.aliyuncs.com",
                "id",
                "secret",
                new Properties(),
                (element, output) -> { });
        try {
            Field producerField = AliyunLogSinkWriter.class.getDeclaredField("producer");
            producerField.setAccessible(true);
            ProducerCredentialsInitializationTest.assertDefaultProducerUserAgent(
                    (Producer) producerField.get(writer));
        } finally {
            writer.close();
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
