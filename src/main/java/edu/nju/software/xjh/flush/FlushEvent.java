package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;

public class FlushEvent implements DBEvent {
    public DBEventType getType() {
        return DBEventType.FLUSH;
    }
}
