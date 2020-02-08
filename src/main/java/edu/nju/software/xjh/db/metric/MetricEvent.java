package edu.nju.software.xjh.db.metric;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;

public class MetricEvent implements DBEvent {
    public DBEventType getType() {
        return DBEventType.METRIC;
    }
}
