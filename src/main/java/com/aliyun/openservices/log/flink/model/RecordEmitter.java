package com.aliyun.openservices.log.flink.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RecordEmitter<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RecordEmitter.class);

    private final BlockingQueue<SourceRecord<T>> queue;
    private final LogDataFetcher<T> fetcher;
    private final CheckpointCommitter committer;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final long idleInterval;
    private final MemoryLimiter memoryLimiter;

    public RecordEmitter(BlockingQueue<SourceRecord<T>> queue,
                         LogDataFetcher<T> fetcher,
                         CheckpointCommitter committer,
                         long idleInterval,
                         MemoryLimiter memoryLimiter) {
        this.queue = queue;
        this.fetcher = fetcher;
        this.committer = committer;
        this.idleInterval = idleInterval;
        this.memoryLimiter = memoryLimiter;
    }

    public void produce(SourceRecord<T> record) throws InterruptedException {
        while (fetcher.isRunning()) {
            if (queue.offer(record, idleInterval, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    @Override
    public void run() {
        while (fetcher.isRunning()) {
            try {
                SourceRecord<T> record = queue.poll(idleInterval, TimeUnit.MILLISECONDS);
                if (record == null) {
                    continue;
                }
                fetcher.emitRecordAndUpdateState(record.getRecord(),
                        record.getTimestamp(),
                        record.getSubscribedShardStateIndex(),
                        record.getNextCursor());
                if (committer != null) {
                    committer.updateCheckpoint(record.getShard(), record.getNextCursor(), record.isReadOnly());
                }
                if (memoryLimiter != null) {
                    memoryLimiter.release(record.getDataRawSize());
                }
            } catch (Exception ex) {
                LOG.error("Fail to emit record {}", ex.getMessage(), ex);
                latch.countDown();
                fetcher.stopWithError(ex);
                return;
            }
        }
        latch.countDown();
        LOG.warn("Record emitter exited");
    }

    public void waitForIdle() throws InterruptedException {
        LOG.warn("Record emitter waiting for idle");
        latch.await();
        LOG.warn("Record emitter idle");
    }
}