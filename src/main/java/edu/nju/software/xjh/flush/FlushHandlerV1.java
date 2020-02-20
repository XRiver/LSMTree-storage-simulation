package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.compaction.CompactionEvent;
import edu.nju.software.xjh.db.Bus;
import edu.nju.software.xjh.db.Config;
import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.db.VersionSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class FlushHandlerV1 extends Thread implements FlushHandler {

    public Logger LOG = LogManager.getLogger(FlushHandlerV1.class);

    private final VersionSet versionSet;
    private final Bus bus;
    private final String fileDir;

    /*
    在目前单一DB（shard）的情况下，每个Handler只需要单线程执行任务而非一个线程池
     */
    private FlushExecutorV1 flushThread;
    private BlockingQueue<FlushEvent> eventQueue;
    private AtomicBoolean running;

    public FlushHandlerV1(DB db) {
        this.bus = db.getBus();
        this.versionSet = db.getVersionSet();
        this.fileDir = db.getConfig().getVal(Config.ConfigVar.FILE_BASE_PATH);

        this.flushThread = new FlushExecutorV1(db.getConfig(), versionSet, fileDir);
        this.eventQueue = new LinkedBlockingQueue<>();
        this.running = new AtomicBoolean(false);
    }

    @Override
    public synchronized void handleEvent(FlushEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void initAndStart() {
        running.set(true);
        this.start();
    }

    @Override
    public void run() {
        while (running.get()) {
            FlushEvent event = eventQueue.poll();
            if (event == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
                continue;
            }

            LOG.debug("Waiting flush thread to become free...");
            while (flushThread.isBusy()) {}

            LOG.debug("Assigning task to flush thread.");
            flushThread.doFlush(event.getRecordList());
            bus.push(new CompactionEvent(0));
        }
        LOG.info("Flush handler shut down");
    }

    @Override
    public void stopFlush() {
        running.set(false);
    }
}
