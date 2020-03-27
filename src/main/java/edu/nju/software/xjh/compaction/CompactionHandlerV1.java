package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.*;
import edu.nju.software.xjh.model.FileMeta;
import edu.nju.software.xjh.util.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class CompactionHandlerV1 extends Thread implements CompactionHandler {
    public Logger LOG = LogManager.getLogger(CompactionHandlerV1.class);

    private final DB db;
    private final VersionSet versionSet;
    private final Config config;
    private Bus bus;
    private final int levelCount;
    private final int lastLevel;
    private final int[] levelMinSize;
    private final int[] compactionMaxPickCount;
    private final long[] compactionMaxPickSize;

    private CompactionExecutor compactionExecutorV1;
    private BlockingQueue<CompactionEvent> eventQueue;
    private AtomicBoolean running;

    public CompactionHandlerV1(DB db) {
        this.db = db;
        this.versionSet = db.getVersionSet();
        this.config = db.getConfig();

        this.levelCount = Integer.parseInt(config.getVal(Config.ConfigVar.LEVEL_COUNT));
        this.lastLevel = levelCount - 1;
        this.levelMinSize = new int[this.levelCount];
        this.compactionMaxPickCount = new int[this.levelCount];
        this.compactionMaxPickSize = new long[this.levelCount];

        String[] minSizes = config.getVal(Config.ConfigVar.LEVEL_MIN_SIZE).split(",");
        String[] maxPickCount = config.getVal(Config.ConfigVar.COMPACTION_MAX_PICK_COUNT).split(",");
        String[] maxPickSize = config.getVal(Config.ConfigVar.COMPACTION_MAX_PICK_SIZE).split(",");
        for (int level = 0; level < this.levelCount; level++) {
            this.levelMinSize[level] = Integer.parseInt(minSizes[level]);
            this.compactionMaxPickCount[level] = Integer.parseInt(maxPickCount[level]);
            this.compactionMaxPickSize[level] = Long.parseLong(maxPickSize[level]) * 1024 * 1024;
        }

        this.eventQueue = new LinkedBlockingQueue<>();
        this.running = new AtomicBoolean(false);
    }

    public void handleEvent(CompactionEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            CompactionEvent event = eventQueue.poll();
            if (event == null) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
                LOG.warn("No compaction events available!");
                continue;
            }

            Version version = versionSet.getCurrentVersion();
            List<FileMeta> fileMetaList = version.getFileMetaList();
            int level = event.getLevel();
            LOG.info("Handling compaction event: level " + event.getLevel());

            List<FileMeta> upperLevelFiles = null;
            List<FileMeta> lowerLevelFiles = null;

            if (level < lastLevel) { // 非最后一层需要与下一层进行合并
                List<FileMeta> basicFiles = new ArrayList<>();
                for (FileMeta fileMeta : fileMetaList) {
                    if (fileMeta.getLevel() == level) {
                        basicFiles.add(fileMeta);
                    }
                }

                if (level == 0) { // 第0层的数据必须先对旧的数据（fileId小的）做compaction
                    if (basicFiles.size() > levelMinSize[0]) {
                        basicFiles.sort(Comparator.naturalOrder());
                        upperLevelFiles = basicFiles.subList(0, compactionMaxPickCount[0]);
                    }
                } else { // 第1、2层则优先选择尺寸小的文件，减少零碎
                    if (basicFiles.size() > levelMinSize[level]) {
                        int pickCount = 0;
                        long pickSize = 0;
                        basicFiles.sort(Comparator.comparingLong(FileMeta::getFileSize));

                        upperLevelFiles = new ArrayList<>();
                        while (pickCount < compactionMaxPickCount[level] &&
                                pickSize < compactionMaxPickSize[level]) {
                            FileMeta next = basicFiles.remove(0);
                            upperLevelFiles.add(next);
                            pickCount++;
                            pickSize += next.getFileSize();
                        }
                    }
                }

                if (upperLevelFiles != null) {
                    byte[] leftEnd = CommonUtils.smallestStartKey(upperLevelFiles);
                    byte[] rightEnd = CommonUtils.largestEndKey(upperLevelFiles);

                    //跟据上层文件的记录rk范围选取下层文件
                    lowerLevelFiles = new ArrayList<>();
                    for (FileMeta fileMeta : fileMetaList) {
                        if (fileMeta.getLevel() == level + 1 && CommonUtils.overlapInRange(fileMeta, leftEnd, rightEnd)) {
                            lowerLevelFiles.add(fileMeta);
                        }
                    }
                    lowerLevelFiles.sort(Comparator.naturalOrder());
                }
            } else { // 最后一层与自己合并
                fileMetaList.removeIf(fileMeta -> fileMeta.getLevel() < lastLevel);

                if (fileMetaList.size() > levelMinSize[lastLevel]) {
                    // 最下一层compaction使用的文件必须是连续的
                    CommonUtils.sortByStartKey(fileMetaList);

                    // 最下一层的一次compaction应该能够减少文件数，否则不做
                    // 所以需要判断文件大小
                    String[] split = config.getVal(Config.ConfigVar.SEGMENT_MAX_SIZE).split(",");
                    long fileSize = Long.parseLong(split[split.length - 1]) * 1024 * 1024;
                    int targetPickCount = compactionMaxPickCount[lastLevel];
                    long sizeLimit = fileSize * (targetPickCount - 1);

                    for (int i = 0; i <= fileMetaList.size() - targetPickCount; i ++) {
                        if (totalSize(fileMetaList.subList(i, i + targetPickCount)) < sizeLimit) {
                            upperLevelFiles = fileMetaList.subList(i, i + targetPickCount);
                            break;
                        }
                    }
                }
            }

            if (upperLevelFiles == null || upperLevelFiles.isEmpty()) {
                LOG.info("Abandoned compaction schedule.");
            } else {
                LOG.info("Scheduled a compaction task on level " + level);
                compactionExecutorV1.doCompactionV1(upperLevelFiles, lowerLevelFiles, level);
            }
        }
        LOG.info("Stopping compaction.");
    }

    private long totalSize(List<FileMeta> files) {
        long ret = 0;
        for (FileMeta f : files) {
            ret += f.getFileSize();
        }
        return ret;

    }

    @Override
    public void initAndStart() {
        this.bus = db.getBus();
        this.compactionExecutorV1 = new CompactionExecutor(config, versionSet, bus);

        this.running.set(true);
        this.start();
    }

    @Override
    public void stopCompaction() {
        running.set(false);
    }
}
