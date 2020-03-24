package com.aliyun.openservices.log.flink.sample;


import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class LogstorePatternTest {

    @Test
    public void testMatchLogstore() throws Exception {
        Pattern pattern = Pattern.compile("logstore-perf-.*");
        assertTrue(pattern.matcher("logstore-perf-1").matches());
    }
}
