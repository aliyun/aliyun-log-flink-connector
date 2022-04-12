package com.aliyun.openservices.log.flink.internal;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ShardHashAdjusterTest {

    @Test
    public void testAdjust() {
        ShardHashAdjuster adjuster = new ShardHashAdjuster(1);
        String adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("00000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("00000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(2);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("80000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("00000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(4);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("c0000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("40000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(8);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("e0000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("60000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(16);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("f0000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("60000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(32);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("f0000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("68000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(64);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("f4000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("6c000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(128);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("f4000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("6e000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(256);
        adjustedShardHash = adjuster.adjust("127.0.0.1");
        assertEquals("f5000000000000000000000000000000", adjustedShardHash);
        adjustedShardHash = adjuster.adjust("192.168.0.2");
        assertEquals("6f000000000000000000000000000000", adjustedShardHash);
    }

    @Test
    public void testAdjustEmptyStr() {
        ShardHashAdjuster adjuster = new ShardHashAdjuster(1);
        String adjustedShardHash = adjuster.adjust("");
        assertEquals("00000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(2);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("80000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(4);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("c0000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(8);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("c0000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(16);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("d0000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(32);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("d0000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(64);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("d4000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(128);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("d4000000000000000000000000000000", adjustedShardHash);

        adjuster = new ShardHashAdjuster(256);
        adjustedShardHash = adjuster.adjust("");
        assertEquals("d4000000000000000000000000000000", adjustedShardHash);
    }
}
