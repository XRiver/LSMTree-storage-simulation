package edu.nju.software.xjh.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 管理DB当前的数据文件信息
 */
public class VersionSet {

    public Logger LOG = LogManager.getLogger(VersionSet.class);

    private Version currentVersion;
    private AtomicLong nextFileId;
    private AtomicLong nextVersionId;

    /* 不包含DB重启功能，每次创建DB认为存储文件为空 */
    public VersionSet() {
        this.nextFileId = new AtomicLong(0);
        this.nextVersionId = new AtomicLong(0);
        this.currentVersion = new Version(getNextVersionId());
    }

    private long getNextVersionId() {
        return nextVersionId.incrementAndGet();
    }

    public long getNextFileId() {
        return nextFileId.incrementAndGet();
    }

    public synchronized void applyNewVersion(VersionMod mod) {
        LOG.debug("Applying version mod:"+mod);
        Version next = new Version(getNextVersionId());
        currentVersion.applyModAsBase(next, mod);
        currentVersion = next;
    }

    public Version getCurrentVersion() {
        return currentVersion;
    }
}
