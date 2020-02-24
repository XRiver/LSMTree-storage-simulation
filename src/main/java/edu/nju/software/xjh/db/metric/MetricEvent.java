package edu.nju.software.xjh.db.metric;

import edu.nju.software.xjh.db.event.DBEvent;
import edu.nju.software.xjh.db.event.DBEventType;

public class MetricEvent implements DBEvent {
    private final DBEventType type;
    private final int createAtLevel;

    private int deletedFiles = 0;
    private int createdFiles = 0;
    private long deletedBytes = 0;
    private long createdBytes = 0;


    public MetricEvent(DBEventType opType, int createAtLevel) {
        this.type = opType;
        this.createAtLevel = createAtLevel;
    }

    public int getDeletedFiles() {
        return deletedFiles;
    }

    public void setDeletedFiles(int deletedFiles) {
        this.deletedFiles = deletedFiles;
    }

    public int getCreatedFiles() {
        return createdFiles;
    }

    public void setCreatedFiles(int createdFiles) {
        this.createdFiles = createdFiles;
    }

    public long getDeletedBytes() {
        return deletedBytes;
    }

    public void setDeletedBytes(long deletedBytes) {
        this.deletedBytes = deletedBytes;
    }

    public long getCreatedBytes() {
        return createdBytes;
    }

    public void setCreatedBytes(long createdBytes) {
        this.createdBytes = createdBytes;
    }

    public int getCreateAtLevel() {
        return createAtLevel;
    }

    public DBEventType getMetricType() {
        return type;
    }

    public DBEventType getType() {
        return DBEventType.METRIC;
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "type=" + type +
                ", createAtLevel=" + createAtLevel +
                ", deletedFiles=" + deletedFiles +
                ", createdFiles=" + createdFiles +
                ", deletedBytes=" + deletedBytes +
                ", createdBytes=" + createdBytes +
                '}';
    }
}
