package edu.nju.software.xjh.compaction;

public interface CompactionHandler {
    void handleEvent(CompactionEvent event);
    void init();
}
