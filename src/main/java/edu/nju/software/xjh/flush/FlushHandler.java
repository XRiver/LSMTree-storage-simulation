package edu.nju.software.xjh.flush;

public interface FlushHandler {
    void handleEvent(FlushEvent event);
    void initAndStart();
    void stopFlush();
}
