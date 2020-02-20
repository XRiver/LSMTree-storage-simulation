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
    private SolutionVersion solutionVersion;

    private VersionSet versionSet;

    public DB(Config config, String name) {
        this.config = config;
        this.name = name;

        this.solutionVersion = SolutionVersion.valueOf(config.getVal(Config.ConfigVar.SOLUTION_VERSION));

        this.recordList = new RecordList(this, config);
        this.compactionHandler = CompactionHandlerFactory.createCompactionHandler(solutionVersion, this);
        this.flushHandler = FlushHandlerFactory.createFlushHandler(solutionVersion, this);
        this.metricHandler = new MetricHandler();
        this.bus = new Bus(compactionHandler, flushHandler, metricHandler);

        this.versionSet = new VersionSet();
    }

    public void init() {
        LOG.info("Start running DB with solution version = " + solutionVersion);

        flushHandler.initAndStart();
        compactionHandler.initAndStart();

        bus.start();
    }

    public void stop() {
        bus.stopBus();
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

    public VersionSet getVersionSet() {
        return versionSet;
    }
}
