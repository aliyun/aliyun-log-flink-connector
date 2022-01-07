package com.aliyun.openservices.log.flink.internal;

import org.junit.Assert;
import org.junit.Test;

public class LogGroupKeyTest {

    @Test
    public void testLogGroupKey() {
        LogGroupKey key1 = new LogGroupKey(null, null, null);
        Assert.assertEquals(key1.getKey(), "$$");
        LogGroupKey key2 = new LogGroupKey("foo", "bar", "xx");
        Assert.assertEquals(key2.getKey(), "foo$bar$xx");
        LogGroupKey key3 = new LogGroupKey("foo", null, null);
        Assert.assertEquals(key3.getKey(), "foo$$");
        LogGroupKey key4 = new LogGroupKey(null, "bar", null);
        Assert.assertEquals(key4.getKey(), "$bar$");
        LogGroupKey key5 = new LogGroupKey(null, null, "xx");
        Assert.assertEquals(key5.getKey(), "$$xx");
        LogGroupKey key6 = new LogGroupKey("foo", null, "xx");
        Assert.assertEquals(key6.getKey(), "foo$$xx");
    }
}
