package edu.nju.software.xjh.db;

import edu.nju.software.xjh.flush.FlushEvent;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.util.SkipList;

class RecordList {

    private SkipList skipList;
    private DB db;

    private int maxCount;

    public RecordList(DB db, Config config) {
        this.maxCount = Integer.parseInt(config.getVal(Config.ConfigVar.RECORD_LIST_SIZE));
        this.skipList = new SkipList();
    }


    public void put(Record record) {
        // 放进跳表
        skipList.put(record);
        // 如果数量达到阈值，则创建flush event
        if (skipList.getNodeNum() >= maxCount) {
            db.getBus().push(new FlushEvent(skipList));
            skipList = new SkipList();
        }
    }
}
