package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.*;
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

    private static String encodeCursor(long offset) {
        byte[] cursorAsBytes = Base64.getEncoder().encode(String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        return new String(cursorAsBytes, StandardCharsets.UTF_8);
    }

    public RawLogGroupList deserialize(PullLogsResult record) {
        RawLogGroupList logGroupList = new RawLogGroupList();
        List<LogGroupData> logGroups = record.getLogGroupList();
        if (logGroups.isEmpty()) {
            return logGroupList;
        }
        long offset = Long.parseLong(record.getReadLastCursor()) - logGroups.size() + 1;
        String lastCursor = encodeCursor(offset);
        String seqNoPrefix = record.getShard() + "|";
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
                    String seqNum = seqNoPrefix + lastCursor + "|" + lIdx;
                    rlog.addContent(sequenceNumberKey, seqNum);
                }
                rawLogGroup.addLog(rlog);
            }
            logGroupList.add(rawLogGroup);
            lastCursor = encodeCursor(++offset);
        }
        return logGroupList;
    }

    public TypeInformation<RawLogGroupList> getProducedType() {
        return PojoTypeInfo.of(RawLogGroupList.class);
    }
}
