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
        LogGroupKey key1 = new LogGroupKey(null, null, null, tags);
        assertEquals(key1.getKey(), "$$");
        LogGroupKey key2 = new LogGroupKey("foo", "bar", "xx", tags);
        assertEquals(key2.getKey(), "foo$bar$xx");
        LogGroupKey key3 = new LogGroupKey("foo", null, null, tags);
        assertEquals(key3.getKey(), "foo$$");
        LogGroupKey key4 = new LogGroupKey(null, "bar", null, tags);
        assertEquals(key4.getKey(), "$bar$");
        LogGroupKey key5 = new LogGroupKey(null, null, "xx", tags);
        assertEquals(key5.getKey(), "$$xx");
        LogGroupKey key6 = new LogGroupKey("foo", null, "xx", tags);
        assertEquals(key6.getKey(), "foo$$xx");
        tags.add(new TagContent("t1", "v1"));
        LogGroupKey key7 = new LogGroupKey(null, null, null, tags);
        assertEquals(key7.getKey(), "$$$t1#v1");
        LogGroupKey key8 = new LogGroupKey("foo", "bar", "xx", tags);
        assertEquals(key8.getKey(), "foo$bar$xx$t1#v1");
        LogGroupKey key9 = new LogGroupKey("foo", null, null, tags);
        assertEquals(key9.getKey(), "foo$$$t1#v1");
        LogGroupKey key10 = new LogGroupKey(null, "bar", null, tags);
        assertEquals(key10.getKey(), "$bar$$t1#v1");
        LogGroupKey key11 = new LogGroupKey(null, null, "xx", tags);
        assertEquals(key11.getKey(), "$$xx$t1#v1");
        LogGroupKey key12 = new LogGroupKey("foo", null, "xx", tags);
        assertEquals(key12.getKey(), "foo$$xx$t1#v1");
        tags.add(new TagContent("t0", "v0"));
        LogGroupKey key13 = new LogGroupKey(null, null, null, tags);
        assertEquals(key13.getKey(), "$$$t0#v0$t1#v1");
        LogGroupKey key14 = new LogGroupKey("foo", "bar", "xx", tags);
        assertEquals(key14.getKey(), "foo$bar$xx$t0#v0$t1#v1");
        LogGroupKey key15 = new LogGroupKey("foo", null, null, tags);
        assertEquals(key15.getKey(), "foo$$$t0#v0$t1#v1");
        LogGroupKey key16 = new LogGroupKey(null, "bar", null, tags);
        assertEquals(key16.getKey(), "$bar$$t0#v0$t1#v1");
        LogGroupKey key17 = new LogGroupKey(null, null, "xx", tags);
        assertEquals(key17.getKey(), "$$xx$t0#v0$t1#v1");
        LogGroupKey key18 = new LogGroupKey("foo", null, "xx", tags);
        assertEquals(key18.getKey(), "foo$$xx$t0#v0$t1#v1");
    }
}
