package edu.nju.software.xjh.db.metric;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * 处理模拟写入过程中的性能统计信息。为性能分析提供依据。
 */
public class MetricHandler {
    private List<MetricEvent> metricEvents = new ArrayList<>();

    public Logger LOG = LogManager.getLogger(MetricHandler.class);

    private long l0CompactionTotalReadBytes = 0;
    private long l0CompactionTotalWriteBytes = 0;
    private int l0CompactionTotalReadCount = 0;
    private int l0CompactionTotalWriteCount = 0;

    private long l0CompactionL0Consumption = 0;
    private long l0CompactionL1Consumption = 0;


    public MetricHandler() {
        new Report().start();
    }

    class Report extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    sleep(2000);
                    LOG.warn(MetricHandler.this::toString);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
    }

    public void handleEvent(MetricEvent event) {
        metricEvents.add(event);
        if (event.getCreateAtLevel() == 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(event.toString());
            }
            l0CompactionTotalReadBytes += event.getDeletedBytes();
            l0CompactionTotalReadCount += event.getDeletedFiles();
            l0CompactionTotalWriteBytes += event.getCreatedBytes();
            l0CompactionTotalWriteCount += event.getCreatedFiles();

            l0CompactionL0Consumption += event.getUpperConsumedBytes();
            l0CompactionL1Consumption += event.getLowerConsumedBytes();
        }
    }


    @Override
    public String toString() {
        return "MetricHandler{" +
                "l0CompactionTotalReadBytes=" + l0CompactionTotalReadBytes +
                ", l0CompactionTotalWriteBytes=" + l0CompactionTotalWriteBytes +
                ", l0CompactionTotalReadCount=" + l0CompactionTotalReadCount +
                ", l0CompactionTotalWriteCount=" + l0CompactionTotalWriteCount +
                ", l0CompactionL0Consumption=" + l0CompactionL0Consumption +
                ", l0CompactionL1Consumption=" + l0CompactionL1Consumption +
                '}';
    }
}
