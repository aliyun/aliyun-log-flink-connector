package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.FastLogGroup;

import java.util.ArrayList;
import java.util.List;

public class FastLogGroupList {
    private List<FastLogGroup> logGroups;

    public FastLogGroupList(int capacity) {
        logGroups = new ArrayList<>(capacity);
    }

    public FastLogGroupList(List<FastLogGroup> logGroups) {
        this.logGroups = logGroups;
    }

    public void addLogGroup(FastLogGroup logGroup) {
        logGroups.add(logGroup);
    }

    public List<FastLogGroup> getLogGroups() {
        return logGroups;
    }

    public void setLogGroups(List<FastLogGroup> logGroups) {
        this.logGroups = logGroups;
    }
}
