package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private List<IOWorker> workers;
    private volatile boolean isStopped = false;
    private final ProducerConfig producerConfig;
    private final Semaphore semaphore;

    public ProducerImpl(ProducerConfig producerConfig) {
        this.cache = new ConcurrentHashMap<>();
        this.queue = new LinkedBlockingQueue<>(producerConfig.getProducerQueueSize());
        this.producerConfig = producerConfig;
        this.clientProxy = new LogClientProxy(
                producerConfig.getEndpoint(),
                producerConfig.getAccessKeyId(),
                producerConfig.getAccessKeySecret(),
                "Flink-Connector-producer-" + ConfigConstants.FLINK_CONNECTOR_VERSION);
        int maxSizeInBytes = producerConfig.getTotalSizeInBytes();
        this.logGroupSizeThreshold = producerConfig.getLogGroupSize();
        this.logGroupMaxLines = producerConfig.getLogGroupMaxLines();
        this.semaphore = new Semaphore(maxSizeInBytes);
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
                    LOG.warn("Flush thread interrupted");
                    break;
                }
            }
        }

        public void stop() {
            isStopped = true;
        }
    }

    private static class IOWorker implements Runnable {
        private final BlockingQueue<ProducerEvent> queue;
        private volatile boolean isStopped = false;
        private final LogClientProxy clientProxy;
        private final String project;
        private final String logstore;
        private final Semaphore semaphore;

        private IOWorker(BlockingQueue<ProducerEvent> queue,
                         LogClientProxy clientProxy,
                         String project,
                         String logstore,
                         Semaphore semaphore) {
            this.queue = queue;
            this.clientProxy = clientProxy;
            this.project = project;
            this.logstore = logstore;
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            LOG.info("IOWorker started.");
            while (!isStopped) {
                try {
                    ProducerEvent event = queue.take();
                    if (event.isPoisonPill()) {
                        LOG.warn("Poison pill event received.");
                        break;
                    }
                    LogGroupHolder logGroup = event.getLogGroup();
                    semaphore.release(logGroup.getSizeInBytes());
                    LOG.debug("Send {} to sls", logGroup.getLogs().size());
                    clientProxy.putLogs(project, logstore, logGroup.getTopic(),
                            logGroup.getSource(), logGroup.getHashKey(), logGroup.getLogs());
                } catch (InterruptedException ex) {
                    LOG.warn("IOWorker interrupted");
                    break;
                } catch (LogException ex) {
                    LOG.error("Error putting data", ex);
                }
            }
            LOG.info("IOWorker exited");
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
            IOWorker worker = new IOWorker(
                    queue,
                    clientProxy,
                    producerConfig.getProject(),
                    producerConfig.getLogstore(),
                    semaphore);
            workers.add(worker);
            threadPool.submit(worker);
        }
    }

    private boolean shouldSend(int bytes, int rows) {
        return bytes >= logGroupSizeThreshold || rows >= logGroupMaxLines;
    }

    @Override
    public void send(String topic, String source, String shardHash, List<LogItem> logItems) throws InterruptedException {
        if (logItems == null || logItems.isEmpty()) {
            return;
        }
        if (isStopped) {
            throw new IllegalStateException("Producer is stopped");
        }
        int bytes = 0;
        List<LogItem> buffer = new ArrayList<>();
        for (LogItem item : logItems) {
            long bytesForItem = 0;
            for (LogContent f : item.GetLogContents()) {
                bytesForItem += f.mKey.length();
                if (f.mValue != null) {
                    bytesForItem += f.mValue.length();
                }
            }
            // TODO What if bytesForItem > 10M?
            buffer.add(item);
            bytes += bytesForItem;
            if (shouldSend(bytes, buffer.size())) {
                semaphore.acquire(bytes);
                LOG.debug("Add to queue {}", buffer.size());
                queue.put(ProducerEvent.makeEvent(new LogGroupHolder(source, topic, shardHash, buffer, bytes)));
                buffer = new ArrayList<>();
                bytes = 0;
            }
        }
        if (buffer.isEmpty()) {
            return;
        }
        semaphore.acquire(bytes);

        LogGroupKey logGroupKey = new LogGroupKey(source, topic, shardHash);
        String key = logGroupKey.getKey();

        lock.lock();
        try {
            LogGroupHolder prev = cache.get(key);
            if (prev != null) {
                prev.addLogs(buffer, bytes);
                if (shouldSend(prev.getSizeInBytes(), prev.getLogs().size())) {
                    queue.put(ProducerEvent.makeEvent(prev));
                    LOG.debug("Add to queue {}", prev.getLogs().size());
                    cache.remove(key);
                }
                return;
            }
            cache.put(key, new LogGroupHolder(source, topic, shardHash, buffer, bytes));
        } finally {
            lock.unlock();
        }
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
            LOG.debug("Add {} logs to queue", holder.getLogs().size());
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
        for (IOWorker worker : workers) {
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
