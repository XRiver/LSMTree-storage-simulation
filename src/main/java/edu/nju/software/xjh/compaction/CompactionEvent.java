package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;

public class CompactionEvent implements DBEvent {
    public DBEventType getType() {
        return DBEventType.COMPACTION;
    }
}
