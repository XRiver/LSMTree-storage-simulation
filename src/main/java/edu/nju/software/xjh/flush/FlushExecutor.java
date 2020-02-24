package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.util.SkipList;

public interface FlushExecutor {
    void doFlush(SkipList records);
    boolean isBusy();
}
