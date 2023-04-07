package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.TagContent;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LogGroupKeyTest {

    @Test
    public void testLogGroupKey() {
        List<TagContent> tags = new ArrayList<>();
        String logstore = "test-logstore";
        assertEquals("test-logstore$$$", LogGroupKey.buildKey(logstore, null, null, null, tags));
        assertEquals("test-logstore$foo$bar$xx", LogGroupKey.buildKey(logstore, "foo", "bar", "xx", tags));
        assertEquals("test-logstore$foo$$", LogGroupKey.buildKey(logstore, "foo", null, null, tags));
        assertEquals("test-logstore$$bar$", LogGroupKey.buildKey(logstore, null, "bar", null, tags));
        assertEquals("test-logstore$$$xx", LogGroupKey.buildKey(logstore, null, null, "xx", tags));
        assertEquals("test-logstore$foo$$xx", LogGroupKey.buildKey(logstore, "foo", null, "xx", tags));

        tags.add(new TagContent("t1", "v1"));
        assertEquals("test-logstore$$$$t1#v1", LogGroupKey.buildKey(logstore, null, null, null, tags));
        assertEquals("test-logstore$foo$bar$xx$t1#v1", LogGroupKey.buildKey(logstore, "foo", "bar", "xx", tags));
        assertEquals("test-logstore$foo$$$t1#v1", LogGroupKey.buildKey(logstore, "foo", null, null, tags));
        assertEquals("test-logstore$$bar$$t1#v1", LogGroupKey.buildKey(logstore, null, "bar", null, tags));
        assertEquals("test-logstore$$$xx$t1#v1", LogGroupKey.buildKey(logstore, null, null, "xx", tags));
        assertEquals("test-logstore$foo$$xx$t1#v1", LogGroupKey.buildKey(logstore, "foo", null, "xx", tags));

        tags.add(new TagContent("t0", "v0"));
        assertEquals("test-logstore$$$$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, null, null, null, tags));
        assertEquals("test-logstore$foo$bar$xx$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, "foo", "bar", "xx", tags));
        assertEquals("test-logstore$foo$$$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, "foo", null, null, tags));
        assertEquals("test-logstore$$bar$$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, null, "bar", null, tags));
        assertEquals("test-logstore$$$xx$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, null, null, "xx", tags));
        assertEquals("test-logstore$foo$$xx$t0#v0$t1#v1", LogGroupKey.buildKey(logstore, "foo", null, "xx", tags));
    }
}
