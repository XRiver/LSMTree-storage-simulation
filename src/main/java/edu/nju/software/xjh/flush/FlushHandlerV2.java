package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.DB;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.LinkedBlockingQueue;

class FlushHandlerV2 extends FlushHandlerV1 implements FlushHandler {

    public FlushHandlerV2(DB db) {
        super(db);
        LOG = LogManager.getLogger(FlushHandlerV2.class);
    }

    @Override
    public void initAndStart() {
        this.versionSet = db.getVersionSet();
        this.bus = db.getBus();

        this.flushThread = new FlushExecutorV2(db.getConfig(), db.getBus(), versionSet, fileDir);
        this.eventQueue = new LinkedBlockingQueue<>();

        running.set(true);
        this.start();
    }
}
