package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class RawLogGroupListDeserializer implements LogDeserializationSchema<RawLogGroupList> {

    public RawLogGroupList deserialize(List<LogGroupData> logGroups) {
        RawLogGroupList logGroupList = new RawLogGroupList();
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
                rawLogGroup.addLog(rlog);
            }
            logGroupList.add(rawLogGroup);
        }
        return logGroupList;
    }

    public TypeInformation<RawLogGroupList> getProducedType() {
        return PojoTypeInfo.of(RawLogGroupList.class);
    }
}
