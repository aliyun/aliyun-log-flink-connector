package com.aliyun.openservices.log.flink.util;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class LogClientProxyTest {

    @Test
    public void testMatchLogstore() throws Exception {
        Pattern pattern = Pattern.compile("logstore-perf-.*");
        List<String> logstores = new ArrayList<>();
        logstores.add("logstore-1");
        List<String> result = new ArrayList<>();
        LogClientProxy.filter(logstores, pattern, result);
        Assert.assertTrue(result.isEmpty());
        logstores.add("logstore-perf-1");
        LogClientProxy.filter(logstores, pattern, result);
        Assert.assertEquals(1, result.size());
    }
}
