package com.aliyun.openservices.log.flink.source.enumerator;

import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.LogstoreShardMeta;
import com.aliyun.openservices.log.flink.source.StartingPosition;
import com.aliyun.openservices.log.flink.source.enumerator.assigner.ModuloSplitAssigner;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.LogUtil;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Enumerator for Aliyun Log Source that discovers shards and assigns them to readers.
 */
public class AliyunLogSourceEnumerator implements SplitEnumerator<AliyunLogSourceSplit, AliyunLogSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogSourceEnumerator.class);

    private final SplitEnumeratorContext<AliyunLogSourceSplit> context;
    private final String project;
    private final String logstore;
    private final String accessKeyId;
    private final String accessKey;
    private final Properties configProps;
    private LogClientProxy logClient;
    private final AliyunLogSplitAssigner splitAssigner;
    private final String consumerGroup;
    private final StartingPosition startingPosition;
    private final StartingPosition defaultPosition;
    private final String timestampPosition;
    private final long discoveryIntervalMs;
    private final String stopTime;

    // Track assigned splits per reader
    private final Map<Integer, Set<AliyunLogSourceSplit>> assignedSplits = new HashMap<>();
    // Track all discovered splits
    private final Map<String, AliyunLogSourceSplit> discoveredSplits = new HashMap<>();
    private boolean consumerGroupCreated = false;
    private final SplitAssignerContext splitAssignerContext;
    private final Set<Integer> readersWithRestoredAssignments = new HashSet<>();

    public AliyunLogSourceEnumerator(
            SplitEnumeratorContext<AliyunLogSourceSplit> context,
            String project,
            String logstore,
            String accessKeyId,
            String accessKey,
            Properties configProps,
            AliyunLogSplitAssigner splitAssigner,
            @Nullable AliyunLogSourceEnumState checkpoint) {
        this.context = context;
        this.project = project;
        this.logstore = logstore;
        this.accessKeyId = accessKeyId;
        this.accessKey = accessKey;
        this.configProps = configProps;
        this.splitAssigner = splitAssigner != null ? splitAssigner : new ModuloSplitAssigner();
        this.consumerGroup = configProps.getProperty(ConfigConstants.LOG_CONSUMERGROUP);
        String initialPositionStr = configProps.getProperty(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_BEGIN_CURSOR);
        this.startingPosition = StartingPosition.fromString(initialPositionStr);
        String defaultPositionStr = LogUtil.getDefaultPosition(configProps);
        this.defaultPosition = defaultPositionStr != null ? StartingPosition.fromString(defaultPositionStr) : StartingPosition.EARLIEST;
        // If starting position is TIMESTAMP, extract the timestamp value
        if (this.startingPosition == StartingPosition.TIMESTAMP) {
            try {
                this.timestampPosition = initialPositionStr;
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid timestamp position: " + initialPositionStr, e);
            }
        } else {
            this.timestampPosition = null;
        }
        this.discoveryIntervalMs = LogUtil.getDiscoveryIntervalMs(configProps);
        this.stopTime = configProps.getProperty(ConfigConstants.STOP_TIME);

        // Restore state from checkpoint if available
        if (checkpoint != null) {
            LOG.info("Restoring enumerator state from checkpoint");
            // Restore assigned splits
            Map<Integer, Set<AliyunLogSourceSplit>> checkpointAssignedSplits = checkpoint.getAssignedSplits();
            if (checkpointAssignedSplits != null) {
                for (Map.Entry<Integer, Set<AliyunLogSourceSplit>> entry : checkpointAssignedSplits.entrySet()) {
                    assignedSplits.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }
                LOG.info("Restored {} assigned split groups from checkpoint", checkpointAssignedSplits.size());
            }

            // Restore discovered splits (both assigned and pending)
            Map<String, AliyunLogSourceSplit> checkpointPendingSplits = checkpoint.getPendingSplits();
            if (checkpointPendingSplits != null) {
                for (AliyunLogSourceSplit split : checkpointPendingSplits.values()) {
                    discoveredSplits.put(split.splitId(), split);
                }
                LOG.info("Restored {} pending splits from checkpoint", checkpointPendingSplits.size());
            }

            // Also add all assigned splits to discovered splits
            for (Set<AliyunLogSourceSplit> splits : assignedSplits.values()) {
                for (AliyunLogSourceSplit split : splits) {
                    discoveredSplits.put(split.splitId(), split);
                }
            }
            LOG.info("Total discovered splits after restore: {}", discoveredSplits.size());
        }
        this.splitAssignerContext = new SplitAssignerContext(context, assignedSplits);
    }

    @Override
    public void start() {
        LOG.info("Starting AliyunLogSourceEnumerator");
        this.logClient = LogClientProxy.makeClient(configProps, accessKeyId, accessKey, context.currentParallelism());
        createConsumerGroupIfConfigured();
        // If we restored from checkpoint, we need to reassign splits to registered readers
        // Otherwise, discover and assign new splits
        if (!assignedSplits.isEmpty() || !discoveredSplits.isEmpty()) {
            LOG.info("Restored state detected: {} assigned split groups, {} discovered splits. Reassigning splits to registered readers.",
                    assignedSplits.size(), discoveredSplits.size());
            moveOutOfRangeAssignmentsToPending();
            reassignRestoredSplitsToRegisteredReaders();
            assignPendingSplits();
        } else {
            // Fresh start - discover and assign splits
            discoverAndAssignSplits();
        }

        schedulePeriodicDiscovery();
    }

    /**
     * Reassigns splits that were previously assigned according to the checkpoint.
     * This is called when restoring from a checkpoint to ensure splits are properly
     * assigned to the currently registered readers.
     */
    private void reassignRestoredSplitsToRegisteredReaders() {
        context.registeredReaders().keySet().forEach(this::assignRestoredSplitsToReader);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("Received split request from subtask {}", subtaskId);
        assignRestoredSplitsToReader(subtaskId);
        // Assign any pending splits to this reader
        assignPendingSplitsToReader(subtaskId);
    }

    @Override
    public void addSplitsBack(List<AliyunLogSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        // Remove from assigned splits
        splits.forEach(assignedSplits.computeIfAbsent(subtaskId, k -> new HashSet<>())::remove);
        // Add back to discovered splits for reassignment
        for (AliyunLogSourceSplit split : splits) {
            discoveredSplits.put(split.splitId(), split);
        }
        // Try to reassign
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {}", subtaskId);
        assignedSplits.putIfAbsent(subtaskId, new HashSet<>());
        assignRestoredSplitsToReader(subtaskId);
        // Assign any pending splits to this new reader
        assignPendingSplitsToReader(subtaskId);
    }

    @Override
    public AliyunLogSourceEnumState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting enumerator state at checkpoint {}", checkpointId);
        Map<String, AliyunLogSourceSplit> pendingSplits = new HashMap<>();
        for (AliyunLogSourceSplit split : discoveredSplits.values()) {
            boolean isAssigned = false;
            for (Set<AliyunLogSourceSplit> assigned : assignedSplits.values()) {
                if (assigned.contains(split)) {
                    isAssigned = true;
                    break;
                }
            }
            if (!isAssigned) {
                pendingSplits.put(split.splitId(), split);
            }
        }
        return new AliyunLogSourceEnumState(assignedSplits, pendingSplits);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing AliyunLogSourceEnumerator");
        if (logClient != null) {
            logClient.close();
        }
    }

    private void schedulePeriodicDiscovery() {
        context.callAsync(
                this::discoverSplits,
                (newSplits, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error during periodic shard discovery", throwable);
                        return;
                    }
                    addDiscoveredSplitsAndAssign(newSplits);
                },
                discoveryIntervalMs,
                discoveryIntervalMs);
    }

    private void discoverAndAssignSplits() {
        try {
            addDiscoveredSplitsAndAssign(discoverSplits());
        } catch (Exception e) {
            LOG.error("Failed to discover Aliyun Log shards for project {} and logstore {}.",
                    project, logstore, e);
            throw new RuntimeException("Failed to discover Aliyun Log shards for project "
                    + project + " and logstore " + logstore + ".", e);
        }
    }

    private List<AliyunLogSourceSplit> discoverSplits() throws Exception {
        LOG.info("Starting shard discovery for project={}, logstore={}", project, logstore);
        List<LogstoreShardMeta> shards = discoverShards();
        LOG.info("Discovered {} shards", shards.size());

        List<AliyunLogSourceSplit> newSplits = createSplitsForShards(shards);
        LOG.info("Created {} splits from shards", newSplits.size());
        return newSplits;
    }

    private void addDiscoveredSplitsAndAssign(List<AliyunLogSourceSplit> newSplits) {
        if (newSplits == null || newSplits.isEmpty()) {
            return;
        }
        int newSplitCount = 0;
        for (AliyunLogSourceSplit split : newSplits) {
            if (!discoveredSplits.containsKey(split.splitId())) {
                LOG.info("Discovered new shard: {} (shardId: {}, cursor: {})",
                        split.splitId(), split.getShardId(), split.getNextCursor());
                discoveredSplits.put(split.splitId(), split);
                newSplitCount++;
            }
        }
        LOG.info("Added {} new splits, total discovered splits: {}", newSplitCount, discoveredSplits.size());
        assignPendingSplits();
    }

    private List<LogstoreShardMeta> discoverShards() throws Exception {
        createConsumerGroupIfConfigured();

        List<LogstoreShardMeta> shardMetas = new ArrayList<>();
        LOG.debug("Listing shards for project={}, logstore={}", project, logstore);
        List<Shard> shards = logClient.listShards(project, logstore);
        LOG.debug("Found {} shards from API", shards.size());

        for (Shard shard : shards) {
            LogstoreShardMeta shardMeta = new LogstoreShardMeta(
                    logstore, shard.getShardId(), shard.getStatus());

            if (shardMeta.isReadOnly() && shardMeta.getEndCursor() == null) {
                String endCursor = logClient.getEndCursor(project, logstore, shard.GetShardId());
                shardMeta.setEndCursor(endCursor);
                LOG.debug("Got end cursor for read-only shard {}: {}", shard.GetShardId(), endCursor);
            }

            shardMetas.add(shardMeta);
            LOG.debug("Added shard meta: shardId={}, status={}, readOnly={}",
                    shard.getShardId(), shard.getStatus(), shardMeta.isReadOnly());
        }

        return shardMetas;
    }

    private List<AliyunLogSourceSplit> createSplitsForShards(List<LogstoreShardMeta> shards) throws Exception {
        List<AliyunLogSourceSplit> splits = new ArrayList<>();
        LOG.debug("Creating splits for {} shards", shards.size());

        for (LogstoreShardMeta shardMeta : shards) {
            String splitId = shardMeta.getId();

            LOG.debug("Creating split candidate: {}, fetching cursors", splitId);
            String initialCursor = getInitialCursor(shardMeta);
            if (initialCursor == null) {
                LOG.warn("Could not get initial cursor for shard {}, skipping", shardMeta.getId());
                continue;
            }
            LOG.debug("Got initial cursor for shard {}: {}", shardMeta.getId(), initialCursor);

            // Get stop cursor only for new splits
            String stopCursor = getStopCursor(shardMeta);
            if (stopCursor != null) {
                LOG.debug("Got stop cursor for shard {}: {}", shardMeta.getId(), stopCursor);
            }

            AliyunLogSourceSplit split = new AliyunLogSourceSplit(shardMeta, initialCursor, stopCursor);
            LOG.debug("Created split {} for shard {} with cursor: {}", split.splitId(), shardMeta.getId(), initialCursor);
            splits.add(split);
        }

        LOG.info("Created {} new splits from {} shards", splits.size(), shards.size());
        return splits;
    }

    private void moveOutOfRangeAssignmentsToPending() {
        int parallelism = context.currentParallelism();
        Iterator<Map.Entry<Integer, Set<AliyunLogSourceSplit>>> iterator = assignedSplits.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Set<AliyunLogSourceSplit>> entry = iterator.next();
            int readerId = entry.getKey();
            if (readerId < 0 || readerId >= parallelism) {
                LOG.warn("Reader {} from checkpoint is outside current parallelism {}, moving {} splits back to pending",
                        readerId, parallelism, entry.getValue().size());
                iterator.remove();
            }
        }
    }

    private void assignRestoredSplitsToReader(int readerId) {
        if (!readersWithRestoredAssignments.add(readerId)) {
            return;
        }
        Set<AliyunLogSourceSplit> splits = assignedSplits.get(readerId);
        if (splits == null || splits.isEmpty()) {
            return;
        }
        List<AliyunLogSourceSplit> splitsToAssign = new ArrayList<>(splits);
        LOG.info("Reassigning {} restored splits to reader {}: {}", splitsToAssign.size(), readerId,
                splitsToAssign.stream().map(AliyunLogSourceSplit::splitId).collect(java.util.stream.Collectors.toList()));
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(readerId, splitsToAssign)));
    }

    private String getInitialCursor(LogstoreShardMeta shardMeta) throws Exception {
        // Check if we have a checkpoint from server-side
        if (startingPosition == StartingPosition.CHECKPOINT) {
            if (consumerGroup != null && !consumerGroup.isEmpty()) {
                String checkpoint = logClient.fetchCheckpoint(
                        project, shardMeta.getLogstore(), consumerGroup, shardMeta.getShardId());
                if (checkpoint != null && !checkpoint.isEmpty()) {
                    LOG.info("Restored cursor from server-side checkpoint for shard {}: {}", shardMeta.getId(), checkpoint);
                    return checkpoint;
                }
            }
            // Fallback to default position
            LOG.info("No server-side checkpoint found for shard {}, using default position: {}", shardMeta.getId(), defaultPosition);
            return getCursorForPosition(shardMeta, defaultPosition);
        }

        return getCursorForPosition(shardMeta, startingPosition);
    }

    private String getCursorForPosition(LogstoreShardMeta shardMeta, StartingPosition position) throws Exception {
        LOG.debug("Getting cursor for shard {} with position {}", shardMeta.getId(), position);
        switch (position) {
            case EARLIEST:
                String beginCursor = logClient.getBeginCursor(project, shardMeta.getLogstore(), shardMeta.getShardId());
                LOG.debug("Got EARLIEST cursor for shard {}: {}", shardMeta.getId(), beginCursor);
                return beginCursor;
            case LATEST:
                String endCursor = logClient.getEndCursor(project, shardMeta.getLogstore(), shardMeta.getShardId());
                LOG.debug("Got LATEST cursor for shard {}: {}", shardMeta.getId(), endCursor);
                return endCursor;
            case TIMESTAMP:
                if (timestampPosition == null) {
                    throw new RuntimeException("Timestamp position specified but no timestamp value provided");
                }
                try {
                    int timestamp = Integer.parseInt(timestampPosition);
                    String timestampCursor = logClient.getCursorAtTimestamp(project, shardMeta.getLogstore(), shardMeta.getShardId(), timestamp);
                    LOG.debug("Got TIMESTAMP cursor for shard {} at {}: {}", shardMeta.getId(), timestamp, timestampCursor);
                    return timestampCursor;
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid timestamp: " + timestampPosition, e);
                }
            case CHECKPOINT:
                // This should not happen here, but handle it gracefully
                if (consumerGroup != null && !consumerGroup.isEmpty()) {
                    String checkpoint = logClient.fetchCheckpoint(
                            project, shardMeta.getLogstore(), consumerGroup, shardMeta.getShardId());
                    if (checkpoint != null && !checkpoint.isEmpty()) {
                        LOG.debug("Got CHECKPOINT cursor for shard {}: {}", shardMeta.getId(), checkpoint);
                        return checkpoint;
                    }
                }
                LOG.debug("No checkpoint found for shard {}, using default position: {}", shardMeta.getId(), defaultPosition);
                return getCursorForPosition(shardMeta, defaultPosition);
            default:
                throw new RuntimeException("Unknown starting position: " + position);
        }
    }

    private String getStopCursor(LogstoreShardMeta shardMeta) {
        if (stopTime != null && !stopTime.isEmpty()) {
            try {
                int timestamp = Integer.parseInt(stopTime);
                try {
                    return logClient.getCursorAtTimestamp(project, shardMeta.getLogstore(), shardMeta.getShardId(), timestamp);
                } catch (Exception e) {
                    LOG.warn("Failed to get stop cursor for shard {}", shardMeta.getId(), e);
                }
            } catch (NumberFormatException e) {
                LOG.warn("Invalid stop time: {}", stopTime);
            }
        }
        return null;
    }

    private void assignPendingSplits() {
        LOG.debug("Assigning pending splits to {} registered readers", context.registeredReaders().size());
        context.registeredReaders().keySet().forEach(this::assignPendingSplitsToReader);
    }

    private void assignPendingSplitsToReader(int readerId) {
        int totalReaders = context.currentParallelism();
        Set<AliyunLogSourceSplit> readerSplits = assignedSplits.computeIfAbsent(readerId, k -> new HashSet<>());

        LOG.debug("Assigning pending splits to reader {} (totalReaders: {}, already assigned: {})",
                readerId, totalReaders, readerSplits.size());

        List<AliyunLogSourceSplit> splitsToAssign = new ArrayList<>();
        for (AliyunLogSourceSplit split : discoveredSplits.values()) {
            // Check if already assigned
            boolean alreadyAssigned = false;
            for (Set<AliyunLogSourceSplit> assigned : assignedSplits.values()) {
                if (assigned.contains(split)) {
                    alreadyAssigned = true;
                    break;
                }
            }

            if (!alreadyAssigned) {
                splitsToAssign.add(split);
            } else {
                LOG.debug("Split {} already assigned, skipping", split.splitId());
            }
        }
        // Check if this split should be assigned to this reader
        List<AliyunLogSourceSplit> assignToReaders = splitAssigner.assign(splitsToAssign, readerId, splitAssignerContext);
        if (!assignToReaders.isEmpty()) {
            LOG.info("Assigning {} splits to reader {}: {}", assignToReaders.size(), readerId,
                    assignToReaders.stream().map(AliyunLogSourceSplit::splitId).collect(java.util.stream.Collectors.toList()));
            readerSplits.addAll(assignToReaders);
            context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(readerId, assignToReaders)));
        } else {
            LOG.debug("No splits to assign to reader {}", readerId);
        }
    }

    private static class SplitAssignerContext implements AliyunLogSplitAssigner.Context {
        private final SplitEnumeratorContext<AliyunLogSourceSplit> splitEnumeratorContext;
        private final Map<Integer, Set<AliyunLogSourceSplit>> splitAssignment;

        public SplitAssignerContext(SplitEnumeratorContext<AliyunLogSourceSplit> splitEnumeratorContext,
                                    Map<Integer, Set<AliyunLogSourceSplit>> splitAssignment) {
            this.splitEnumeratorContext = splitEnumeratorContext;
            this.splitAssignment = splitAssignment;
        }

        @Override
        public Map<Integer, Set<AliyunLogSourceSplit>> getCurrentSplitAssignment() {
            return splitAssignment;
        }

        @Override
        public SplitEnumeratorContext<AliyunLogSourceSplit> getSplitEnumeratorContext() {
            return splitEnumeratorContext;
        }
    }

    private void createConsumerGroupIfNotExist() {
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            return;
        }
        try {
            boolean exists = logClient.checkConsumerGroupExists(project, logstore, consumerGroup);
            if (!exists) {
                LOG.info("Creating consumer group {} for project {} and logstore {}.",
                        consumerGroup, project, logstore);
                logClient.createConsumerGroup(project, logstore, consumerGroup);
            }
        } catch (Exception e) {
            LOG.error("Failed to create or check consumer group {} for project {} and logstore {}.",
                    consumerGroup, project, logstore, e);
            throw new RuntimeException("Failed to create or check consumer group "
                    + consumerGroup + " for project " + project + " and logstore " + logstore + ".", e);
        }
    }

    private void createConsumerGroupIfConfigured() {
        if (!consumerGroupCreated && hasConsumerGroup()) {
            LOG.info("Checking consumer group {} before shard discovery.", consumerGroup);
            createConsumerGroupIfNotExist();
            consumerGroupCreated = true;
        }
    }

    static boolean hasConsumerGroup(String consumerGroup) {
        return consumerGroup != null && !consumerGroup.isEmpty();
    }

    private boolean hasConsumerGroup() {
        return hasConsumerGroup(consumerGroup);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No-op for now
    }
}
