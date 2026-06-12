package com.aliyun.openservices.log.flink.source.table;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.deserialization.AliyunLogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Aliyun Log Service pull results into one RowData record per log line.
 */
public class AliyunLogRowDataDeserializationSchema implements AliyunLogDeserializationSchema<RowData> {
    public static final String FIELD_TIME = "__time__";
    public static final String FIELD_TOPIC = "__topic__";
    public static final String FIELD_SOURCE = "__source__";
    public static final String FIELD_SHARD = "__shard__";
    public static final String FIELD_CURSOR = "__cursor__";

    private final RowType rowType;
    private final List<RowType.RowField> fields;
    private final boolean ignoreParseErrors;

    public AliyunLogRowDataDeserializationSchema(RowType rowType, boolean ignoreParseErrors) {
        this.rowType = rowType;
        this.fields = rowType.getFields();
        this.ignoreParseErrors = ignoreParseErrors;
    }

    @Override
    public void deserialize(PullLogsResult record, Collector<RowData> out) {
        for (LogGroupData logGroupData : record.getLogGroupList()) {
            FastLogGroup logGroup = logGroupData.GetFastLogGroup();
            for (int logIndex = 0; logIndex < logGroup.getLogsCount(); logIndex++) {
                FastLog log = logGroup.getLogs(logIndex);
                out.collect(createRow(record, logGroup, log));
            }
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(rowType);
    }

    private RowData createRow(PullLogsResult record, FastLogGroup logGroup, FastLog log) {
        Map<String, String> contents = contentMap(log);
        GenericRowData row = new GenericRowData(RowKind.INSERT, fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            String rawValue = getRawValue(record, logGroup, log, contents, fieldName);
            row.setField(i, convert(rawValue, field.getType(), fieldName, log));
        }
        return row;
    }

    private Map<String, String> contentMap(FastLog log) {
        Map<String, String> contents = new HashMap<>(log.getContentsCount());
        for (int i = 0; i < log.getContentsCount(); i++) {
            FastLogContent content = log.getContents(i);
            contents.put(content.getKey(), content.getValue());
        }
        return contents;
    }

    private String getRawValue(
            PullLogsResult record,
            FastLogGroup logGroup,
            FastLog log,
            Map<String, String> contents,
            String fieldName) {
        switch (fieldName) {
            case FIELD_TIME:
                return String.valueOf(log.getTime());
            case FIELD_TOPIC:
                return logGroup.getTopic();
            case FIELD_SOURCE:
                return logGroup.getSource();
            case FIELD_SHARD:
                return String.valueOf(record.getShard());
            case FIELD_CURSOR:
                return record.getCursor();
            default:
                return contents.get(fieldName);
        }
    }

    private Object convert(String value, LogicalType type, String fieldName, FastLog log) {
        if (value == null) {
            return null;
        }
        try {
            LogicalTypeRoot root = type.getTypeRoot();
            switch (root) {
                case CHAR:
                case VARCHAR:
                    return StringData.fromString(value);
                case BOOLEAN:
                    return parseBoolean(value);
                case TINYINT:
                    return Byte.parseByte(value);
                case SMALLINT:
                    return Short.parseShort(value);
                case INTEGER:
                    return Integer.parseInt(value);
                case BIGINT:
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) type;
                    return DecimalData.fromBigDecimal(
                            new BigDecimal(value),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                case DATE:
                    return convertDate(value);
                case TIME_WITHOUT_TIME_ZONE:
                    return convertTime(value);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return convertTimestamp(value, fieldName, log);
                case BINARY:
                case VARBINARY:
                    return value.getBytes(StandardCharsets.UTF_8);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported field type " + type.asSummaryString() + " for field " + fieldName);
            }
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException) {
                throw (UnsupportedOperationException) e;
            }
            if (ignoreParseErrors) {
                return null;
            }
            throw new IllegalArgumentException(
                    "Failed to parse field '" + fieldName + "' value '" + value + "' as "
                            + type.asSummaryString(),
                    e);
        }
    }

    private boolean parseBoolean(String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private int convertDate(String value) {
        if (isInteger(value)) {
            return Integer.parseInt(value);
        }
        return (int) LocalDate.parse(value).toEpochDay();
    }

    private int convertTime(String value) {
        if (isInteger(value)) {
            return Integer.parseInt(value);
        }
        return (int) (LocalTime.parse(value).toNanoOfDay() / 1_000_000L);
    }

    private TimestampData convertTimestamp(String value, String fieldName, FastLog log) {
        if (FIELD_TIME.equals(fieldName)) {
            int timeNsPart = normalizeTimeNsPart(log.getTimeNsPart());
            long epochMillis = log.getTime() * 1000L + timeNsPart / 1_000_000L;
            int nanoOfMillisecond = timeNsPart % 1_000_000;
            return TimestampData.fromEpochMillis(epochMillis, nanoOfMillisecond);
        }
        if (isInteger(value)) {
            return TimestampData.fromEpochMillis(Long.parseLong(value));
        }
        try {
            return TimestampData.fromLocalDateTime(LocalDateTime.parse(value));
        } catch (Exception ignored) {
            return TimestampData.fromTimestamp(Timestamp.valueOf(value));
        }
    }

    private static boolean isInteger(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        int start = value.charAt(0) == '-' ? 1 : 0;
        if (start == value.length()) {
            return false;
        }
        for (int i = start; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static int normalizeTimeNsPart(int timeNsPart) {
        if (timeNsPart < 0 || timeNsPart >= 1_000_000_000) {
            return 0;
        }
        return timeNsPart;
    }
}
