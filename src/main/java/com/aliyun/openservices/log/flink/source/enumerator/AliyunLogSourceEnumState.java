package com.aliyun.openservices.log.flink.source.enumerator;

import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * State for the AliyunLogSourceEnumerator.
 * Tracks which splits are assigned to which readers.
 */
public class AliyunLogSourceEnumState implements Serializable {
    private static final long serialVersionUID = 1L;

    // Map of reader ID to assigned splits
    private final Map<Integer, Set<AliyunLogSourceSplit>> assignedSplits;
    // Pending splits that haven't been assigned yet
    private final Map<String, AliyunLogSourceSplit> pendingSplits;

    public AliyunLogSourceEnumState() {
        this.assignedSplits = new HashMap<>();
        this.pendingSplits = new HashMap<>();
    }

    public AliyunLogSourceEnumState(Map<Integer, Set<AliyunLogSourceSplit>> assignedSplits,
                                    Map<String, AliyunLogSourceSplit> pendingSplits) {
        this.assignedSplits = assignedSplits != null ? new HashMap<>(assignedSplits) : new HashMap<>();
        this.pendingSplits = pendingSplits != null ? new HashMap<>(pendingSplits) : new HashMap<>();
    }

    public Map<Integer, Set<AliyunLogSourceSplit>> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, AliyunLogSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    public AliyunLogSourceEnumState copy() {
        return new AliyunLogSourceEnumState(assignedSplits, pendingSplits);
    }
}


