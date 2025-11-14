package com.aliyun.openservices.log.flink.source.enumerator.assigner;

import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSplitAssigner;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Round-robin split assigner that distributes splits evenly across all readers.
 * <p>
 * This assigner ensures balanced load distribution by assigning splits in a
 * round-robin fashion. It calculates the target number of splits per reader
 * based on the total number of splits (including already assigned ones) and
 * distributes pending splits to balance the load.
 * <p>
 * <b>Use cases:</b>
 * <ul>
 *   <li>When all splits have similar data volume and processing requirements</li>
 *   <li>When you want to maximize parallelism and balance load</li>
 *   <li>When split-to-reader affinity is not important</li>
 * </ul>
 * <p>
 * <b>Example:</b>
 * <pre>{@code
 * AliyunLogSource<String> source = AliyunLogSource.<String>builder()
 *     .setProject("my-project")
 *     .setLogStore("my-logstore")
 *     .setSplitAssigner(new RoundRobinSplitAssigner())
 *     .build();
 * }</pre>
 * <p>
 * <b>Comparison with ModuloSplitAssigner:</b>
 * <ul>
 *   <li><b>RoundRobinSplitAssigner:</b> Balances load evenly, but may reassign splits
 *       when parallelism changes. Best for stateless processing.</li>
 *   <li><b>ModuloSplitAssigner:</b> Deterministic assignment, same shard always goes
 *       to same reader. Best for stateful processing or when split affinity matters.</li>
 * </ul>
 * <p>
 * <b>Note:</b> This assigner may reassign splits when the number of readers
 * changes, unlike {@link ModuloSplitAssigner} which provides deterministic
 * assignment based on shard ID.
 */
public class RoundRobinSplitAssigner implements AliyunLogSplitAssigner {

    /**
     * Assigns pending splits to the specified reader using round-robin distribution.
     * <p>
     * The algorithm:
     * <ol>
     *   <li>Calculates total splits (pending + already assigned)</li>
     *   <li>Determines target splits per reader (ceiling division)</li>
     *   <li>Assigns splits to this reader until it reaches the target count</li>
     * </ol>
     * This ensures that all readers end up with approximately the same number
     * of splits, with at most one split difference.
     *
     * @param pendingSplits the list of splits waiting to be assigned
     * @param readerId      the ID of the reader requesting splits (0-based)
     * @param context       provides access to current assignments and parallelism info
     * @return list of splits assigned to this reader
     */
    @Override
    public List<AliyunLogSourceSplit> assign(List<AliyunLogSourceSplit> pendingSplits, int readerId, Context context) {
        // Get the total number of parallel readers
        int numReaders = context.getSplitEnumeratorContext().currentParallelism();

        // Validate parallelism
        if (numReaders <= 0) {
            throw new IllegalArgumentException("Number of parallel subtasks must be positive, got: " + numReaders);
        }

        // Count total splits: pending splits + already assigned splits
        int totalSplits = pendingSplits.size();
        Map<Integer, Set<AliyunLogSourceSplit>> splitAssignments = context.getCurrentSplitAssignment();
        for (Map.Entry<Integer, Set<AliyunLogSourceSplit>> entry : splitAssignments.entrySet()) {
            totalSplits += entry.getValue().size();
        }

        // Calculate target splits per reader (using ceiling division)
        // Formula: (total + numReaders - 1) / numReaders ensures we round up
        // This guarantees balanced distribution with at most one split difference
        int splitsPerReader = (totalSplits + numReaders - 1) / numReaders;

        // Count how many splits this reader already has
        Set<AliyunLogSourceSplit> assigned = splitAssignments.get(readerId);
        int count = assigned == null ? 0 : assigned.size();

        // Calculate how many more splits this reader needs to reach the target
        List<AliyunLogSourceSplit> toAssign = new ArrayList<>();
        int splitsToAssign = splitsPerReader - count;

        // Assign splits from pending list until we reach the target
        // This ensures balanced load distribution across all readers
        for (AliyunLogSourceSplit split : pendingSplits) {
            if (splitsToAssign <= 0) {
                break;
            }
            toAssign.add(split);
            --splitsToAssign;
        }
        return toAssign;
    }
}
