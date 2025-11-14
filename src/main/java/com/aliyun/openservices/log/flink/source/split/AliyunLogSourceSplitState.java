package com.aliyun.openservices.log.flink.source.split;

public class AliyunLogSourceSplitState {
    private final AliyunLogSourceSplit aliyunLogSourceSplit;
    private String nextCursor;

    public AliyunLogSourceSplitState(AliyunLogSourceSplit aliyunLogSourceSplit) {
        this.aliyunLogSourceSplit = aliyunLogSourceSplit;
        this.nextCursor = aliyunLogSourceSplit.getNextCursor();
    }

    public AliyunLogSourceSplit getAliyunLogSourceSplit() {
        return new AliyunLogSourceSplit(
                aliyunLogSourceSplit.getShardMeta(),
                nextCursor,
                aliyunLogSourceSplit.getStopCursor());
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public void setNextCursor(String nextCursor) {
        this.nextCursor = nextCursor;
    }
}
