package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.model.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 设计为于FlushHandler同步的模式
 */
public class FlushExecutorV2 {

    public Logger LOG = LogManager.getLogger(FlushExecutorV2.class);

    private AtomicBoolean busy = new AtomicBoolean(false);

    public void doFlush(Iterator<Record> records, String fileBasePath, long fileId) {
        if (!busy.compareAndSet(false,true)) {
            LOG.error("Assigned with task while busy!");
            return;
        }




    }

    public boolean isBusy() {
        return busy.get();
    }
}
