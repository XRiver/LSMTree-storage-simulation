package edu.nju.software.xjh.db;

import edu.nju.software.xjh.compaction.CompactionHandler;
import edu.nju.software.xjh.flush.FlushHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DB {
    private static DB instance;
    public Logger LOG = LogManager.getLogger(DB.class);

    private FlushHandler flushHandler;
    private CompactionHandler compactionHandler;

    public static DB getInstance() {
        if (instance == null) {
            instance = new DB();

        }
        return instance;
    }


}
