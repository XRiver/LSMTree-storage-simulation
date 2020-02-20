package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.util.SkipList;

import java.util.Iterator;

public class FlushEvent implements DBEvent {
    private SkipList recordList;
    public FlushEvent(SkipList recordList) {
        this.recordList = recordList;
    }

    public DBEventType getType() {
        return DBEventType.FLUSH;
    }

    /**
     * 事件处理完成时应当清理SkipList
     */
    public SkipList getRecordList() {
        return recordList;
    }
}
