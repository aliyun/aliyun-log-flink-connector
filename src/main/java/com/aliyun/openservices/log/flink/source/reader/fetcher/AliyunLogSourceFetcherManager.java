package com.aliyun.openservices.log.flink.source.reader.fetcher;

import com.aliyun.openservices.log.flink.model.PullLogsResult;
import com.aliyun.openservices.log.flink.source.reader.AliyunLogSplitReader;
import com.aliyun.openservices.log.flink.source.split.AliyunLogSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Fetcher manager for Aliyun Log Service source that extends Flink's SplitFetcherManager.
 * Manages multiple fetchers, each handling one split, and provides checkpoint commit functionality.
 */
public class AliyunLogSourceFetcherManager extends SplitFetcherManager<PullLogsResult, AliyunLogSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunLogSourceFetcherManager.class);

    // Map to track which fetcher handles which split (splitId -> fetcherId)
    private final Map<String, Integer> splitsToFetcherMap;

    public AliyunLogSourceFetcherManager(
            Supplier<SplitReader<PullLogsResult, AliyunLogSourceSplit>> splitReaderSupplier,
            Configuration configuration) {
        super(splitReaderSupplier, configuration);
        this.splitsToFetcherMap = new ConcurrentHashMap<>();
    }

    @Override
    public void addSplits(List<AliyunLogSourceSplit> splitsToAdd) {
        for (AliyunLogSourceSplit split : splitsToAdd) {
            // Ensure splits with same splitId are assigned to the same fetcher
            getFetcher(split)
                    .orElseGet(() -> {
                        SplitFetcher<PullLogsResult, AliyunLogSourceSplit> newFetcher =
                                createSplitFetcher();
                        this.splitsToFetcherMap.put(split.splitId(), newFetcher.fetcherId());
                        startFetcher(newFetcher);
                        return newFetcher;
                    })
                    .addSplits(Collections.singletonList(split));
        }
    }

    @Override
    public void removeSplits(List<AliyunLogSourceSplit> splitsToRemove) {
        for (AliyunLogSourceSplit split : splitsToRemove) {
            getFetcher(split).ifPresent(fetcher -> {
                fetcher.removeSplits(Collections.singletonList(split));
                splitsToFetcherMap.remove(split.splitId());
            });
        }
    }

    /**
     * Get the fetcher for a given split.
     */
    private Optional<SplitFetcher<PullLogsResult, AliyunLogSourceSplit>> getFetcher(AliyunLogSourceSplit split) {
        return Optional.ofNullable(this.splitsToFetcherMap.get(split.splitId()))
                .map(fetcherId -> this.fetchers.get(fetcherId));
    }

    /**
     * Get a running fetcher if available.
     */
    protected SplitFetcher<PullLogsResult, AliyunLogSourceSplit> getRunningFetcher() {
        return this.fetchers.isEmpty() ? null : this.fetchers.values().iterator().next();
    }

    /**
     * Commit checkpoints for multiple splits.
     * This method is called from the source reader when a checkpoint completes.
     *
     * @param checkpointMap map of split to cursor to commit
     * @param callback      callback to invoke for each commit result
     */
    public void commitCheckpoints(
            Map<AliyunLogSourceSplit, String> checkpointMap,
            Consumer<CommitResult> callback) {
        LOG.debug("Committing {} checkpoints", checkpointMap.size());
        if (checkpointMap == null || checkpointMap.isEmpty()) {
            return;
        }

        AtomicInteger committedSplitCount = new AtomicInteger(0);
        checkpointMap.forEach((split, cursor) -> {
            String splitId = split.splitId();

            // Find the fetcher for this split, or use a running fetcher
            SplitFetcher<PullLogsResult, AliyunLogSourceSplit> splitFetcher =
                    getFetcher(split).orElseGet(this::getRunningFetcher);

            Consumer<Throwable> taskCallback = (e) -> {
                if (e == null) {
                    if (committedSplitCount.incrementAndGet() == checkpointMap.size()) {
                        callback.accept(new CommitResult(splitId, true, null));
                    }
                } else {
                    callback.accept(new CommitResult(splitId, false, e));
                }
            };

            if (splitFetcher != null) {
                // The fetcher thread is still running. This should be the majority of the cases.
                enqueueCheckpointCommitTask(splitFetcher, split, cursor, taskCallback);
            } else {
                // Create a new fetcher if none exists
                splitFetcher = createSplitFetcher();
                enqueueCheckpointCommitTask(splitFetcher, split, cursor, taskCallback);
                startFetcher(splitFetcher);
            }
        });
    }

    /**
     * Enqueue a checkpoint commit task to the fetcher.
     */
    private void enqueueCheckpointCommitTask(
            SplitFetcher<PullLogsResult, AliyunLogSourceSplit> splitFetcher,
            AliyunLogSourceSplit split,
            String cursor,
            Consumer<Throwable> callback) {
        AliyunLogSplitReader reader = (AliyunLogSplitReader) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() throws IOException {
                        reader.notifyCheckpointComplete(split, cursor, callback);
                        return true;
                    }

                    @Override
                    public void wakeUp() {
                        // No-op
                    }
                });
    }

    @Override
    public void close(long timeoutMs) throws Exception {
        splitsToFetcherMap.clear();
        super.close(timeoutMs);
    }

    /**
     * Result of a checkpoint commit operation.
     */
    public static class CommitResult {
        public final String splitId;
        public final boolean success;
        public final Throwable error;

        public CommitResult(String splitId, boolean success, Throwable error) {
            this.splitId = splitId;
            this.success = success;
            this.error = error;
        }
    }
}

