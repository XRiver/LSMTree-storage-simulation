package edu.nju.software.xjh.db.metric;

import java.util.ArrayList;
import java.util.List;

/**
 * 处理模拟写入过程中的性能统计信息。为性能分析提供依据。
 */
public class MetricHandler {
    private List<MetricEvent> metricEvents = new ArrayList<>();

    public void handleEvent(MetricEvent event) {
        metricEvents.add(event);
    }

    //TODO 跟据论文需求分析events.
}
