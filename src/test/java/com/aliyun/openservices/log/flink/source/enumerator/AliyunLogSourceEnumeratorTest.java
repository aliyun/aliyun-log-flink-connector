package com.aliyun.openservices.log.flink.source.enumerator;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AliyunLogSourceEnumeratorTest {

    @Test
    public void testHasConsumerGroupReturnsTrueWhenConfigured() {
        assertTrue(AliyunLogSourceEnumerator.hasConsumerGroup("consumer-group"));
    }

    @Test
    public void testHasConsumerGroupReturnsFalseWhenNotConfigured() {
        assertFalse(AliyunLogSourceEnumerator.hasConsumerGroup(null));
        assertFalse(AliyunLogSourceEnumerator.hasConsumerGroup(""));
    }
}
