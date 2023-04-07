package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.TagContent;
import com.aliyun.openservices.log.flink.model.MemoryLimiter;
import com.aliyun.openservices.log.flink.util.Consts;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import com.aliyun.openservices.log.flink.util.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerImpl implements Producer {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerImpl.class);

    private Map<String, LogGroupHolder> cache;
    private final Lock lock = new ReentrantLock();
    private final BlockingQueue<ProducerEvent> queue;
    private final int logGroupSizeThreshold;
    private final int logGroupMaxLines;
    private ExecutorService threadPool;
    private final LogClientProxy clientProxy;
    private FlushWorker flushWorker;
    private List<ProducerWorker> workers;
    private volatile boolean isStopped = false;
    private final ProducerConfig producerConfig;
    private final Semaphore semaphore;
    private final ShardHashAdjuster shardHashAdjuster;

    public ProducerImpl(ProducerConfig producerConfig, RetryPolicy retryPolicy) {
        this.cache = new ConcurrentHashMap<>();
        this.queue = new LinkedBlockingQueue<>(producerConfig.getProducerQueueSize());
        this.producerConfig = producerConfig;
        this.clientProxy = new LogClientProxy(
                producerConfig.getEndpoint(),
                producerConfig.getAccessKeyId(),
                producerConfig.getAccessKeySecret(),
                "Flink-Connector-producer-" + Consts.FLINK_CONNECTOR_VERSION,
                retryPolicy,
                new MemoryLimiter());
        int maxSizeInBytes = producerConfig.getTotalSizeInBytes();
        this.logGroupSizeThreshold = producerConfig.getLogGroupSize();
        this.logGroupMaxLines = producerConfig.getLogGroupMaxLines();
        this.semaphore = new Semaphore(maxSizeInBytes);
        this.shardHashAdjuster = producerConfig.isAdjustShardHash()
                ? new ShardHashAdjuster(producerConfig.getBuckets())
                : null;
    }

    private static class FlushWorker implements Runnable {
        private final long flushInterval;
        private final Producer producer;
        private volatile boolean isStopped = false;

        private FlushWorker(long flushInterval, Producer producer) {
            this.flushInterval = flushInterval;
            this.producer = producer;
        }

        @Override
        public void run() {
            LOG.info("Flush worker started.");
            while (!isStopped) {
                try {
                    Thread.sleep(flushInterval);
                    producer.flush();
                } catch (InterruptedException ex) {
                    if (!isStopped) {
                        LOG.warn("Flush thread interrupted");
                    }
                    break;
                }
            }
        }

        public void stop() {
            isStopped = true;
        }
    }

    @Override
    public void open() {
        LOG.info("Opening producer with config {}", producerConfig.toString());

        int ioThreadNum = producerConfig.getIoThreadNum();
        long flushInterval = producerConfig.getFlushInterval();
        this.threadPool = Executors.newFixedThreadPool(ioThreadNum + 1, new ThreadFactory() {

            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("Producer-" + counter.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
        this.flushWorker = new FlushWorker(flushInterval, this);
        threadPool.submit(flushWorker);
        this.workers = new ArrayList<>();
        for (int i = 0; i < ioThreadNum; i++) {
            ProducerWorker worker = new ProducerWorker(
                    queue,
                    clientProxy,
                    producerConfig.getProject(),
                    semaphore);
            workers.add(worker);
            threadPool.submit(worker);
        }
    }

    private boolean shouldSend(int bytes, int rows) {
        return bytes >= logGroupSizeThreshold || rows >= logGroupMaxLines;
    }

    @Override
    public void send(String logstore,
                     String topic,
                     String source,
                     String shardHash,
                     List<TagContent> tags,
                     List<LogItem> logItems) throws InterruptedException {
        if (logItems == null || logItems.isEmpty()) {
            return;
        }
        if (isStopped) {
            throw new ProducerException("Producer is stopped");
        }
        if (shardHash != null && shardHashAdjuster != null) {
            shardHash = shardHashAdjuster.adjust(shardHash);
        }
        String key = LogGroupKey.buildKey(logstore, source, topic, shardHash, tags);
        lock.lock();
        try {
            LogGroupHolder prev = cache.get(key);
            for (LogItem item : logItems) {
                int size = getSizeOfLogItem(item);
                if (size >= ProducerConfig.MAX_LOG_GROUP_SIZE) {
                    throw new ProducerException("LogItem size is too large: " + size);
                }
                if (shouldSend(size, 1)) {
                    // The current row is large enough
                    if (prev != null) {
                        // flush first to keep order
                        queue.put(ProducerEvent.makeEvent(prev));
                        prev = null;
                    }
                    semaphore.acquire(size);
                    LogGroupHolder tmpHolder = new LogGroupHolder(logstore, source, topic, shardHash, tags,
                            Collections.singletonList(item), size);
                    queue.put(ProducerEvent.makeEvent(tmpHolder));
                    continue;
                }
                semaphore.acquire(size);
                if (prev == null) {
                    List<LogItem> buffer = new ArrayList<>();
                    buffer.add(item);
                    prev = new LogGroupHolder(logstore, source, topic, shardHash, tags, buffer, size);
                    continue;
                }
                prev.pushBack(item, size);
                if (shouldSend(prev.getSizeInBytes(), prev.getCount())) {
                    queue.put(ProducerEvent.makeEvent(prev));
                    prev = null;
                }
            }
            if (prev != null) {
                cache.put(key, prev);
            } else {
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    private static int getSizeOfLogItem(LogItem item) {
        int bytesForItem = 0;
        for (LogContent f : item.GetLogContents()) {
            if (f.mKey != null) {
                bytesForItem += f.mKey.length();
            }
            if (f.mValue != null) {
                bytesForItem += f.mValue.length();
            }
        }
        return bytesForItem;
    }

    @Override
    public void flush() throws InterruptedException {
        LOG.debug("Flushing producer.");
        Map<String, LogGroupHolder> tp;
        lock.lock();
        try {
            if (cache.isEmpty()) {
                return;
            }
            tp = cache;
            cache = new ConcurrentHashMap<>();
        } finally {
            lock.unlock();
        }
        for (Map.Entry<String, LogGroupHolder> entry : tp.entrySet()) {
            LogGroupHolder holder = entry.getValue();
            LOG.debug("Add {} logs to queue", holder.getCount());
            queue.put(ProducerEvent.makeEvent(holder));
        }
    }

    @Override
    public void close() {
        if (isStopped) {
            LOG.warn("Producer is stopped already.");
            return;
        }
        isStopped = true;
        flushWorker.stop();
        try {
            flush();
        } catch (InterruptedException ex) {
            LOG.error("Interrupted while flushing.");
        }
        for (ProducerWorker worker : workers) {
            worker.stop();
        }
        for (int i = 0; i < producerConfig.getIoThreadNum(); ++i) {
            queue.add(ProducerEvent.makePoisonPill());
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            LOG.warn("Thread interrupted");
        }
        clientProxy.close();
    }
}
