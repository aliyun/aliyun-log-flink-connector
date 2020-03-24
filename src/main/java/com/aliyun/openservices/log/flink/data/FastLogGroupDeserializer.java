package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class FastLogGroupDeserializer implements LogDeserializationSchema<FastLogGroupList> {

    @Override
    public FastLogGroupList deserialize(List<LogGroupData> logGroupDataList) {
        int count = logGroupDataList == null ? 0 : logGroupDataList.size();
        FastLogGroupList logGroupList = new FastLogGroupList(count);
        if (logGroupDataList != null && !logGroupDataList.isEmpty()) {
            for (LogGroupData logGroupData : logGroupDataList) {
                logGroupList.addLogGroup(logGroupData.GetFastLogGroup());
            }
        }
        return logGroupList;
    }

    @Override
    public TypeInformation<FastLogGroupList> getProducedType() {
        return PojoTypeInfo.of(FastLogGroupList.class);
    }
}
