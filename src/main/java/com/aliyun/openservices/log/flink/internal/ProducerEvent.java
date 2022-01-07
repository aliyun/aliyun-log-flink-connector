package com.aliyun.openservices.log.flink.internal;

public class ProducerEvent {
    private LogGroupHolder logGroup;
    private boolean isPoisonPill;

    public ProducerEvent(LogGroupHolder logGroup) {
        this(false);
        this.logGroup = logGroup;
    }

    public ProducerEvent(boolean isPoisonPill) {
        this.isPoisonPill = isPoisonPill;
    }

    public static ProducerEvent makeEvent(LogGroupHolder logGroup) {
        return new ProducerEvent(logGroup);
    }

    public static ProducerEvent makePoisonPill() {
        return new ProducerEvent(true);
    }

    public LogGroupHolder getLogGroup() {
        return logGroup;
    }

    public void setLogGroup(LogGroupHolder logGroup) {
        this.logGroup = logGroup;
    }

    public boolean isPoisonPill() {
        return isPoisonPill;
    }

    public void setPoisonPill(boolean poisonPill) {
        isPoisonPill = poisonPill;
    }
}
