package com.aliyun.openservices.log.flink.model;

import com.aliyun.openservices.log.common.LogGroupData;

import java.util.List;

public class PullLogsResult implements java.io.Serializable {
    private List<LogGroupData> logGroupList;
    private int shard;
    private String cursor;
    private String nextCursor;
    private String readLastCursor;
    private int rawSize;
    private int count;
    private long cursorTime;

    public PullLogsResult(List<LogGroupData> logGroupList,
                          int shard,
                          String cursor,
                          String nextCursor,
                          String readLastCursor,
                          int rawSize,
                          int count,
                          long cursorTime) {
        this.logGroupList = logGroupList;
        this.shard = shard;
        this.cursor = cursor;
        this.nextCursor = nextCursor;
        this.readLastCursor = readLastCursor;
        this.rawSize = rawSize;
        this.count = count;
        this.cursorTime = cursorTime;
    }

    public List<LogGroupData> getLogGroupList() {
        return logGroupList;
    }

    public void setLogGroupList(List<LogGroupData> logGroupList) {
        this.logGroupList = logGroupList;
    }

    public int getShard() {
        return shard;
    }

    public void setShard(int shard) {
        this.shard = shard;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }

    public String getReadLastCursor() {
        return readLastCursor;
    }

    public void setReadLastCursor(String readLastCursor) {
        this.readLastCursor = readLastCursor;
    }

    public int getRawSize() {
        return rawSize;
    }

    public void setRawSize(int rawSize) {
        this.rawSize = rawSize;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getCursorTime() {
        return cursorTime;
    }

    public void setCursorTime(long cursorTime) {
        this.cursorTime = cursorTime;
    }
}
