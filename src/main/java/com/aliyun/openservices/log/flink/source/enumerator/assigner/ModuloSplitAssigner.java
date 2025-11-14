package com.aliyun.openservices.log.flink.source.enumerator.assigner;

import com.aliyun.openservices.log.flink.source.enumerator.AliyunLogSplitAssigner;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Modulo-based split assigner that deterministically assigns splits based on shard ID.
 * <p>
 * This assigner uses a modulo operation on the shard ID to determine which reader
 * should handle each split. The assignment is deterministic: the same shard will
 * always be assigned to the same reader (when parallelism is constant), which
 * provides:
 * <ul>
 *   <li><b>Deterministic assignment:</b> Same shard always goes to same reader</li>
 *   <li><b>Stable state:</b> Useful for stateful processing where split affinity matters</li>
 *   <li><b>Predictable distribution:</b> Easy to reason about which reader handles which shard</li>
 * </ul>
 * <p>
 * <b>Use cases:</b>
 * <ul>
 *   <li>When you need deterministic split-to-reader mapping</li>
 *   <li>When split order or affinity is important for your processing logic</li>
 *   <li>When you want to ensure the same shard is always processed by the same reader</li>
 * </ul>
 * <p>
 * <b>Example:</b>
 * <pre>{@code
 * AliyunLogSource<String> source = AliyunLogSource.<String>builder()
 *     .setProject("my-project")
 *     .setLogStore("my-logstore")
 *     .setSplitAssigner(new ModuloSplitAssigner())  // This is the default
 *     .build();
 * }</pre>
 * <p>
 * <b>Note:</b> This is the default assigner used when no custom assigner is specified.
 * The assignment formula ensures that negative shard IDs (if any) are handled correctly
 * by using: {@code ((shardId % totalReaders) + totalReaders) % totalReaders}
 */
public class ModuloSplitAssigner implements AliyunLogSplitAssigner {

    /**
     * Assigns pending splits to the specified reader based on shard ID modulo.
     * <p>
     * For each pending split, calculates which reader should handle it using:
     * {@code reader = ((shardId % totalReaders) + totalReaders) % totalReaders}
     * <p>
     * The double modulo operation ensures correct handling of negative shard IDs
     * (if the shard ID is negative, the first modulo might produce a negative result,
     * so we add totalReaders and modulo again to get a positive result in [0, totalReaders-1]).
     * <p>
     * Only splits whose calculated reader matches the requested readerId are returned.
     *
     * @param pendingSplits the list of splits waiting to be assigned
     * @param readerId      the ID of the reader requesting splits (0-based)
     * @param context       provides access to parallelism information
     * @return list of splits assigned to this reader based on shard ID modulo
     */
    @Override
    public List<AliyunLogSourceSplit> assign(List<AliyunLogSourceSplit> pendingSplits, int readerId, Context context) {
        List<AliyunLogSourceSplit> splits = new ArrayList<>();
        int totalReaders = context.getSplitEnumeratorContext().currentParallelism();

        // For each pending split, determine which reader should handle it
        for (AliyunLogSourceSplit split : pendingSplits) {
            // Calculate target reader using modulo operation
            // The double modulo ensures correct handling of negative shard IDs:
            // 1. First modulo: shardId % totalReaders (may be negative)
            // 2. Add totalReaders: makes it positive if it was negative
            // 3. Second modulo: ensures result is in [0, totalReaders-1]
            int reader = ((split.getShardId() % totalReaders) + totalReaders) % totalReaders;

            // If this split belongs to the requested reader, add it to the result
            if (reader == readerId) {
                splits.add(split);
            }
        }

        return splits;
    }
}
