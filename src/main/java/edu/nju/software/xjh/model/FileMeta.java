package edu.nju.software.xjh.model;

public class FileMeta implements Comparable {
    private long fileId;
    private String filePath;
    private long fileSize;
    private int level;

    private int recordNumber;
    private Record startRecord;
    private Record endRecord;

    private int majorId = -1;

    public FileMeta(long fileId, String filePath) {
        this.fileId = fileId;
        this.filePath = filePath;
    }

    public long getFileId() {
        return fileId;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(int recordNumber) {
        this.recordNumber = recordNumber;
    }

    public Record getStartRecord() {
        return startRecord;
    }

    public void setStartRecord(Record startRecord) {
        this.startRecord = startRecord;
    }

    public Record getEndRecord() {
        return endRecord;
    }

    public void setEndRecord(Record endRecord) {
        this.endRecord = endRecord;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getMajorId() {
        return majorId;
    }

    public void setMajorId(int majorId) {
        this.majorId = majorId;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof FileMeta) {
            FileMeta other = (FileMeta) o;
            if (other == this || (other.fileId == fileId && other.filePath.equals(filePath))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "FileMeta{" +
                "fileId=" + fileId +
                ", filePath='" + filePath + '\'' +
                ", level=" + level +
                ", recordNumber=" + recordNumber +
                ", fileSize=" + fileSize +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof FileMeta) {
            return (int) (fileId - ((FileMeta) o).fileId);
        } else {
            throw new IllegalArgumentException("FileMeta only compares to FileMeta.");
        }
    }
}
