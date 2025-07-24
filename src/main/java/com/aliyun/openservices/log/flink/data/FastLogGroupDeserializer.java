package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class FastLogGroupDeserializer implements LogDeserializationSchema<FastLogGroupList> {

    @Override
    public FastLogGroupList deserialize(PullLogsResult record) {
        List<LogGroupData> logGroupDataList = record.getLogGroupList();
        FastLogGroupList logGroupList = new FastLogGroupList(logGroupDataList.size());
        for (LogGroupData logGroupData : logGroupDataList) {
            logGroupList.addLogGroup(logGroupData.GetFastLogGroup());
        }
        return logGroupList;
    }

    @Override
    public TypeInformation<FastLogGroupList> getProducedType() {
        return PojoTypeInfo.of(FastLogGroupList.class);
    }
}
