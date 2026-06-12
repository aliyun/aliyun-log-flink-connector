package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.flink.data.SinkRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AliyunLogRowDataSerializationSchemaTest {

    @Test
    public void testSerializeMetadataAndContentFields() {
        RowType rowType = new RowType(asList(
                new RowType.RowField(AliyunLogRowDataSerializationSchema.FIELD_TIME, new TimestampType()),
                new RowType.RowField(AliyunLogRowDataSerializationSchema.FIELD_TOPIC, new VarCharType()),
                new RowType.RowField(AliyunLogRowDataSerializationSchema.FIELD_SOURCE, new VarCharType()),
                new RowType.RowField(AliyunLogRowDataSerializationSchema.FIELD_LOGSTORE, new VarCharType()),
                new RowType.RowField(AliyunLogRowDataSerializationSchema.FIELD_HASH_KEY, new VarCharType()),
                new RowType.RowField("level", new VarCharType()),
                new RowType.RowField("status", new IntType())));
        AliyunLogRowDataSerializationSchema schema =
                new AliyunLogRowDataSerializationSchema(rowType, "default-topic", "default-source");

        GenericRowData row = new GenericRowData(RowKind.INSERT, 7);
        row.setField(0, TimestampData.fromEpochMillis(1650000000123L));
        row.setField(1, StringData.fromString("topic-a"));
        row.setField(2, StringData.fromString("source-a"));
        row.setField(3, StringData.fromString("logstore-a"));
        row.setField(4, StringData.fromString("hash-a"));
        row.setField(5, StringData.fromString("INFO"));
        row.setField(6, 200);

        List<SinkRecord> records = collect(schema, row);
        assertEquals(1, records.size());
        SinkRecord record = records.get(0);
        assertEquals("topic-a", record.getTopic());
        assertEquals("source-a", record.getSource());
        assertEquals("logstore-a", record.getLogstore());
        assertEquals("hash-a", record.getHashKey());
        assertEquals(1650000000, record.getLogItem().GetTime());

        Map<String, String> contents = toContentMap(record);
        assertEquals(2, contents.size());
        assertEquals("INFO", contents.get("level"));
        assertEquals("200", contents.get("status"));
    }

    @Test
    public void testUsesDefaultTopicAndSource() {
        RowType rowType = new RowType(asList(
                new RowType.RowField("message", new VarCharType())));
        AliyunLogRowDataSerializationSchema schema =
                new AliyunLogRowDataSerializationSchema(rowType, "default-topic", "default-source");

        GenericRowData row = new GenericRowData(RowKind.INSERT, 1);
        row.setField(0, StringData.fromString("hello"));

        SinkRecord record = collect(schema, row).get(0);
        assertEquals("default-topic", record.getTopic());
        assertEquals("default-source", record.getSource());
        assertEquals("hello", toContentMap(record).get("message"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRejectsNonInsertRows() {
        RowType rowType = new RowType(asList(
                new RowType.RowField("message", new VarCharType())));
        AliyunLogRowDataSerializationSchema schema =
                new AliyunLogRowDataSerializationSchema(rowType, "", null);

        GenericRowData row = new GenericRowData(RowKind.DELETE, 1);
        row.setField(0, StringData.fromString("delete"));

        collect(schema, row);
    }

    private static List<SinkRecord> collect(AliyunLogRowDataSerializationSchema schema, GenericRowData row) {
        List<SinkRecord> records = new ArrayList<>();
        schema.serialize(row, new Collector<SinkRecord>() {
            @Override
            public void collect(SinkRecord record) {
                records.add(record);
            }

            @Override
            public void close() {
            }
        });
        return records;
    }

    private static Map<String, String> toContentMap(SinkRecord record) {
        Map<String, String> contents = new HashMap<>();
        for (LogContent content : record.getLogItem().GetLogContents()) {
            contents.put(content.GetKey(), content.GetValue());
        }
        return contents;
    }

    private static <T> List<T> asList(T... values) {
        List<T> list = new ArrayList<>();
        for (T value : values) {
            list.add(value);
        }
        return list;
    }
}
