package edu.nju.software.xjh.db;

import java.util.ArrayList;
import java.util.List;

/**
 * 某一时间状态下的文件信息
 */
public class Version {

    private long versionId;
    private List<FileMeta> fileMetaList;

    public Version(long versionId) {
        this.versionId = versionId;
        this.fileMetaList = new ArrayList<>();
    }

    public List<FileMeta> getFileMetaList() {
        return new ArrayList<>(fileMetaList);
    }

    public void applyModAsBase(Version empty, VersionMod mod) {
       switch (mod.getType()) {
           case FLUSH:
               empty.fileMetaList.addAll(fileMetaList);
               empty.fileMetaList.addAll(mod.getAddedFiles());
               break;
           case COMPACTION:
               empty.fileMetaList.addAll(fileMetaList);
               empty.fileMetaList.addAll(mod.getAddedFiles());
               empty.fileMetaList.removeAll(mod.getRemovedFiles());
               break;
            default:
                throw new IllegalArgumentException("Version mod's type unknown:" + mod.getType());
       }
    }
}
