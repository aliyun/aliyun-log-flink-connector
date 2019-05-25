package com.aliyun.openservices.log.flink.model;

public enum CheckpointMode {
    /**
     * Never commit checkpoint to remote server.
     */
    DISABLED,
    /**
     * Commit checkpoint only when Flink creating checkpoint, which means Flink
     * checkpointing must be enabled.
     */
    ON_CHECKPOINTS,
    /**
     * Auto commit checkpoint periodic.
     */
    PERIODIC;

    public static CheckpointMode fromString(String value) {
        for (CheckpointMode mode : CheckpointMode.values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Illegal checkpoint mode: " + value);
    }
}
