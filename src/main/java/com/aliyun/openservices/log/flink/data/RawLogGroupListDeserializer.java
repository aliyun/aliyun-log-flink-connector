package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class RawLogGroupListDeserializer implements LogDeserializationSchema<RawLogGroupList> {

    private String sequenceNumberKey;

    public String getSequenceNumberKey() {
        return sequenceNumberKey;
    }

    public void setSequenceNumberKey(String sequenceNumberKey) {
        this.sequenceNumberKey = sequenceNumberKey;
    }

    private static long decodeCursor(String cursor) {
        byte[] timestampAsBytes = Base64.getDecoder().decode(cursor.getBytes(StandardCharsets.UTF_8));
        String timestamp = new String(timestampAsBytes, StandardCharsets.UTF_8);
        return Long.parseLong(timestamp);
    }

    public RawLogGroupList deserialize(PullLogsResult record) {
        RawLogGroupList logGroupList = new RawLogGroupList();
        List<LogGroupData> logGroups = record.getLogGroupList();
        long offset = decodeCursor(record.getCursor());
        String seqNoPrefix = record.getShard() + "_";
        for (LogGroupData logGroup : logGroups) {
            FastLogGroup flg = logGroup.GetFastLogGroup();
            RawLogGroup rawLogGroup = new RawLogGroup();
            rawLogGroup.setSource(flg.getSource());
            rawLogGroup.setTopic(flg.getTopic());
            for (int tagIdx = 0; tagIdx < flg.getLogTagsCount(); ++tagIdx) {
                FastLogTag logtag = flg.getLogTags(tagIdx);
                rawLogGroup.addTag(logtag.getKey(), logtag.getValue());
            }
            for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                FastLog log = flg.getLogs(lIdx);
                RawLog rlog = new RawLog();
                rlog.setTime(log.getTime());
                for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                    FastLogContent content = log.getContents(cIdx);
                    rlog.addContent(content.getKey(), content.getValue());
                }
                if (sequenceNumberKey != null) {
                    String seqNum = seqNoPrefix + offset + "_" + lIdx;
                    rlog.addContent(sequenceNumberKey, seqNum);
                }
                rawLogGroup.addLog(rlog);
            }
            logGroupList.add(rawLogGroup);
            ++offset;
        }
        return logGroupList;
    }

    public TypeInformation<RawLogGroupList> getProducedType() {
        return PojoTypeInfo.of(RawLogGroupList.class);
    }
}
