package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.flink.data.SinkRecord;
import com.aliyun.openservices.log.flink.model.AliyunLogSerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * Converts SQL RowData into one SLS SinkRecord.
 */
public class AliyunLogRowDataSerializationSchema implements AliyunLogSerializationSchema<RowData> {
    public static final String FIELD_TIME = "__time__";
    public static final String FIELD_TOPIC = "__topic__";
    public static final String FIELD_SOURCE = "__source__";
    public static final String FIELD_LOGSTORE = "__logstore__";
    public static final String FIELD_HASH_KEY = "__hash_key__";

    private final List<RowType.RowField> fields;
    private final RowData.FieldGetter[] fieldGetters;
    private final String defaultTopic;
    private final String defaultSource;

    public AliyunLogRowDataSerializationSchema(RowType rowType, String defaultTopic, String defaultSource) {
        this.fields = rowType.getFields();
        this.defaultTopic = defaultTopic;
        this.defaultSource = defaultSource;
        this.fieldGetters = new RowData.FieldGetter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(fields.get(i).getType(), i);
        }
    }

    @Override
    public void serialize(RowData row, Collector<SinkRecord> output) {
        if (row == null) {
            return;
        }
        if (row.getRowKind() != RowKind.INSERT) {
            throw new UnsupportedOperationException("Aliyun Log SQL sink only supports INSERT rows, but got "
                    + row.getRowKind());
        }

        LogItem logItem = new LogItem((int) (System.currentTimeMillis() / 1000L));
        SinkRecord record = new SinkRecord();
        record.setTopic(defaultTopic);
        record.setSource(defaultSource);
        record.setLogItem(logItem);

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            Object value = fieldGetters[i].getFieldOrNull(row);
            if (value == null) {
                continue;
            }
            if (applyMetadata(record, logItem, fieldName, value, field.getType())) {
                continue;
            }
            logItem.PushBack(fieldName, formatValue(value, field.getType()));
        }
        output.collect(record);
    }

    private boolean applyMetadata(
            SinkRecord record,
            LogItem logItem,
            String fieldName,
            Object value,
            LogicalType type) {
        switch (fieldName) {
            case FIELD_TIME:
                logItem.SetTime(toEpochSeconds(value, type));
                return true;
            case FIELD_TOPIC:
                record.setTopic(formatValue(value, type));
                return true;
            case FIELD_SOURCE:
                record.setSource(formatValue(value, type));
                return true;
            case FIELD_LOGSTORE:
                record.setLogstore(formatValue(value, type));
                return true;
            case FIELD_HASH_KEY:
                record.setHashKey(formatValue(value, type));
                return true;
            default:
                return false;
        }
    }

    private int toEpochSeconds(Object value, LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            TimestampData timestamp = (TimestampData) value;
            return (int) (timestamp.getMillisecond() / 1000L);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(formatValue(value, type));
    }

    private String formatValue(Object value, LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
                return ((StringData) value).toString();
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return String.valueOf(value);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return ((DecimalData) value).toBigDecimal()
                        .setScale(decimalType.getScale())
                        .toPlainString();
            case DATE:
                return LocalDate.ofEpochDay(((Integer) value).longValue()).toString();
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((TimestampData) value).toLocalDateTime().toString();
            case BINARY:
            case VARBINARY:
                return new String((byte[]) value, StandardCharsets.UTF_8);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported SQL sink field type: " + type.asSummaryString());
        }
    }
}
