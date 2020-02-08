package edu.nju.software.xjh.db;

import edu.nju.software.xjh.compaction.CompactionHandler;
import edu.nju.software.xjh.compaction.CompactionEvent;
import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.metric.MetricEvent;
import edu.nju.software.xjh.flush.FlushEvent;
import edu.nju.software.xjh.db.metric.MetricHandler;
import edu.nju.software.xjh.flush.FlushHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Bus extends Thread {

    public Logger LOG = LogManager.getLogger(Bus.class);

    private BlockingQueue<DBEvent> eventQueue;
    private AtomicBoolean running = new AtomicBoolean(true);

    private CompactionHandler compactionHandler;
    private FlushHandler flushHandler;
    private MetricHandler metricHandler;

    public Bus(CompactionHandler compactionHandler, FlushHandler flushHandler, MetricHandler metricHandler) {
        this.compactionHandler = compactionHandler;
        this.flushHandler = flushHandler;
        this.metricHandler = metricHandler;

        eventQueue = new LinkedBlockingQueue<DBEvent>();
    }

    public void push(DBEvent event) {
        if (running.get()) {
            eventQueue.offer(event);
        } else {
            LOG.warn("Bus shut down, Rejecting more event: " + event);
        }
    }

    public void run() {
        while (running.get()) {
            DBEvent event = eventQueue.poll();
            if (event != null) {
                handleEvent(event);
            }
        }
    }

    public void stopBus() {
        if (running.compareAndSet(true, false)) {
            int size = eventQueue.size();
            eventQueue.clear();
            LOG.info("Bus stopped with " + size + " events remaining.");
        } else {
            LOG.warn("Bus already shut down.");
        }
    }

    private void handleEvent(DBEvent event) {
        switch (event.getType()) {
            case FLUSH:
                flushHandler.handleEvent((FlushEvent) event);
                break;
            case COMPACTION:
                compactionHandler.handleEvent((CompactionEvent) event);
                break;
            case METRIC:
                metricHandler.handleEvent((MetricEvent) event);
                break;
        }
    }
}
