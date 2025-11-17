package com.aliyun.openservices.log.flink.source;

import java.io.Serializable;

/**
 * Enum for starting position when consuming from Aliyun Log Service.
 */
public enum StartingPosition implements Serializable {
    /**
     * Start from the earliest available log (begin cursor).
     */
    EARLIEST("earliest"),

    /**
     * Start from the latest available log (end cursor).
     */
    LATEST("latest"),

    /**
     * Start from the checkpoint stored on Aliyun Log Service server side.
     * Requires consumer group to be configured.
     */
    CHECKPOINT("checkpoint"),

    /**
     * Start from a specific timestamp (Unix timestamp in seconds).
     * When using this, the timestamp value should be provided separately.
     */
    TIMESTAMP("timestamp");

    private final String value;

    StartingPosition(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parse a string to StartingPosition enum.
     * Supports both enum names and legacy string values.
     *
     * @param str the string to parse (can be enum name, value, or legacy value)
     * @return the corresponding StartingPosition enum value
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public static StartingPosition fromString(String str) {
        if (str == null || str.isEmpty()) {
            return EARLIEST; // default
        }

        String normalized = str.trim().toLowerCase();

        // Check enum names
        for (StartingPosition pos : values()) {
            if (pos.name().equalsIgnoreCase(normalized) || pos.value.equalsIgnoreCase(normalized)) {
                return pos;
            }
        }

        // Support legacy values
        if ("begin_cursor".equalsIgnoreCase(normalized)) {
            return EARLIEST;
        }
        if ("end_cursor".equalsIgnoreCase(normalized)) {
            return LATEST;
        }
        if ("consumer_from_checkpoint".equalsIgnoreCase(normalized)) {
            return CHECKPOINT;
        }

        // If it's a number, treat as timestamp
        try {
            Integer.parseInt(normalized);
            return TIMESTAMP;
        } catch (NumberFormatException e) {
            // Not a number, fall through
        }

        throw new IllegalArgumentException("Unknown starting position: " + str);
    }
}




