package com.aliyun.openservices.log.flink.source.enumerator;

import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for assigning splits (shards) to source readers.
 * <p>
 * Implementations of this interface determine how splits are distributed among
 * multiple parallel readers in a Flink job. Different assignment strategies
 * provide different trade-offs:
 * <ul>
 *   <li><b>RoundRobinSplitAssigner</b>: Distributes splits evenly across readers,
 *       ensuring balanced load. Best for scenarios where all splits have similar
 *       data volume and processing requirements.</li>
 *   <li><b>ModuloSplitAssigner</b>: Assigns splits deterministically based on
 *       shard ID modulo reader count. Ensures the same split always goes to the
 *       same reader, which can be useful for stateful processing or when split
 *       order matters.</li>
 * </ul>
 * <p>
 * Custom implementations can be provided to implement domain-specific assignment
 * logic, such as affinity-based assignment or load-aware distribution.
 */
public interface AliyunLogSplitAssigner extends Serializable {

    /**
     * Assigns pending splits to a specific reader.
     * <p>
     * This method is called by the enumerator for each reader to determine
     * which splits should be assigned to that reader. The implementation should
     * return a list of splits that will be assigned to the specified reader.
     *
     * @param pendingSplits the list of splits that are available for assignment
     * @param readerId the ID of the reader requesting splits (0-based)
     * @param context provides access to current split assignments and enumerator context
     * @return list of splits to assign to the specified reader
     */
    List<AliyunLogSourceSplit> assign(List<AliyunLogSourceSplit> pendingSplits, int readerId, Context context);

    /**
     * Context providing information about the current split assignment state.
     */
    interface Context {
        /**
         * Gets the current split assignments across all readers.
         * <p>
         * The map key is the reader ID, and the value is the set of splits
         * currently assigned to that reader.
         *
         * @return map of reader ID to set of assigned splits
         */
        Map<Integer, Set<AliyunLogSourceSplit>> getCurrentSplitAssignment();

        /**
         * Gets the split enumerator context for accessing Flink runtime information.
         *
         * @return the split enumerator context
         */
        SplitEnumeratorContext<AliyunLogSourceSplit> getSplitEnumeratorContext();
    }
}
