package com.aliyun.openservices.log.flink.internal;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.flink.util.LogClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class ProducerWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerWorker.class);

    private final BlockingQueue<ProducerEvent> queue;
    private volatile boolean isStopped = false;
    private final LogClientProxy clientProxy;
    private final String project;
    private final Semaphore semaphore;

    public ProducerWorker(BlockingQueue<ProducerEvent> queue,
                          LogClientProxy clientProxy,
                          String project,
                          Semaphore semaphore) {
        this.queue = queue;
        this.clientProxy = clientProxy;
        this.project = project;
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        LOG.info("Producer worker started.");
        while (!isStopped) {
            try {
                ProducerEvent event = queue.take();
                if (event.isPoisonPill()) {
                    LOG.warn("Poison pill event received.");
                    break;
                }
                LogGroupHolder logGroup = event.getLogGroup();
                LOG.debug("Send {} logs to sls", logGroup.getCount());
                clientProxy.putLogs(
                        project,
                        logGroup.getLogstore(),
                        logGroup.getTopic(),
                        logGroup.getSource(),
                        logGroup.getHashKey(),
                        logGroup.getTags(),
                        logGroup.getLogs());
                semaphore.release(logGroup.getSizeInBytes());
            } catch (InterruptedException ex) {
                LOG.warn("Producer worker interrupted");
                break;
            } catch (LogException ex) {
                LOG.error(ex.GetErrorMessage(), ex);
                throw new RuntimeException(ex.GetErrorMessage(), ex);
            }
        }
        LOG.info("Producer worker exited");
    }

    public void stop() {
        isStopped = true;
    }
}
