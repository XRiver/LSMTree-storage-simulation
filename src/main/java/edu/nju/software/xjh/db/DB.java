package edu.nju.software.xjh.db;

import edu.nju.software.xjh.compaction.CompactionHandler;
import edu.nju.software.xjh.compaction.CompactionHandlerFactory;
import edu.nju.software.xjh.db.metric.MetricHandler;
import edu.nju.software.xjh.flush.FlushHandler;
import edu.nju.software.xjh.flush.FlushHandlerFactory;
import edu.nju.software.xjh.model.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DB {
    public Logger LOG = LogManager.getLogger(DB.class);

    private Bus bus;

    private FlushHandler flushHandler;
    private CompactionHandler compactionHandler;
    private MetricHandler metricHandler;

    private RecordList recordList;

    private Config config;
    private String name;
    private SolutionVersion version;

    public DB(Config config, String name) {
        this.config = config;
        this.name = name;

        this.version = SolutionVersion.valueOf(config.getVal(Config.ConfigVar.SOLUTION_VERSION));

        this.recordList = new RecordList(this, config);
        //TODO 创建其他成员
        this.compactionHandler = CompactionHandlerFactory.createCompactionHandler(version, this);
        this.flushHandler = FlushHandlerFactory.createFlushHandler(version, this);
        this.metricHandler = new MetricHandler();
        this.bus = new Bus(compactionHandler, flushHandler, metricHandler);
    }

    public void write(Record record) {
        recordList.put(record);
    }

    public FlushHandler getFlushHandler() {
        return flushHandler;
    }

    public CompactionHandler getCompactionHandler() {
        return compactionHandler;
    }

    public MetricHandler getMetricHandler() {
        return metricHandler;
    }

    public RecordList getRecordList() {
        return recordList;
    }

    public Config getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    public Bus getBus() {
        return bus;
    }
}
