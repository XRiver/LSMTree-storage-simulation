package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.Config;
import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.db.VersionSet;
import edu.nju.software.xjh.util.SkipList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class FlushHandlerV2 extends Thread implements FlushHandler {
    public Logger LOG = LogManager.getLogger(FlushHandlerV2.class);

    private VersionSet versionSet;
    private String fileDir;

    /*
    在目前单一DB（shard）的情况下，每个Handler只需要单线程执行任务而非一个线程池
     */
    private FlushExecutorV2 flushThread;
    private BlockingQueue<FlushEvent> eventQueue;
    private AtomicBoolean running;

    public FlushHandlerV2(DB db) {
        versionSet = db.getVersionSet();
        fileDir = db.getConfig().getVal(Config.ConfigVar.FILE_BASE_PATH);
        flushThread = new FlushExecutorV2();
        eventQueue = new LinkedBlockingQueue<>();
        running = new AtomicBoolean(false);
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
            long nextFileId = versionSet.getNextFileId();
            SkipList recordList = event.getRecordList();
            flushThread.doFlush(recordList.toIterator(), fileDir, nextFileId);
        }
        LOG.info("Flush handler shut down");
    }

    @Override
    public void stopFlush() {
        running.set(false);
    }
}
