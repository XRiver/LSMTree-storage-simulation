package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;

public class CompactionEvent implements DBEvent {
    private final int level;
    public CompactionEvent(int level) {
        this.level = level;
    }
    public DBEventType getType() {
        return DBEventType.COMPACTION;
    }

    public int getLevel() {
        return level;
    }
}
