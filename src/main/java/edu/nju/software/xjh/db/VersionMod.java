package edu.nju.software.xjh.db;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 存储文件信息的一次变动
 */
public class VersionMod {
    private VersionModType type;
    private List<FileMeta> addedFiles;
    private List<FileMeta> removedFiles;

    public VersionMod(VersionModType type) {
        this.type = type;
        this.addedFiles = new LinkedList<>();
        this.removedFiles = new LinkedList<>();
    }

    public void addFile(FileMeta f) {
        addedFiles.add(f);
    }

    public void removeFile(FileMeta f) {
        removedFiles.add(f);
    }

    public VersionModType getType() {
        return type;
    }

    public List<FileMeta> getAddedFiles() {
        return addedFiles;
    }

    public List<FileMeta> getRemovedFiles() {
        return removedFiles;
    }

    @Override
    public String toString() {
        return "VersionMod{" +
                "type=" + type +
                ", addedFiles=" + Arrays.toString(addedFiles.toArray()) +
                ", removedFiles=" + Arrays.toString(removedFiles.toArray()) +
                '}';
    }
}

