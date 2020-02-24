package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.*;
import edu.nju.software.xjh.util.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class CompactionHandlerV2 extends Thread implements CompactionHandler {

    public Logger LOG = LogManager.getLogger(CompactionHandlerV2.class);

    private final VersionSet versionSet;
    private final Config config;
    private final Bus bus;
    private final int levelCount;
    private final int lastLevel;
    private final int[] levelMinSize;
    private final int[] compactionMaxPickCount;
    private final long[] compactionMaxPickSize;

    private CompactionExecutor compactionExecutorV1;
    private BlockingQueue<CompactionEvent> eventQueue;
    private AtomicBoolean running;

    public CompactionHandlerV2(DB db) {
        this.versionSet = db.getVersionSet();
        this.bus = db.getBus();
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

        this.compactionExecutorV1 = new CompactionExecutor();
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
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
                continue;
            }

            Version version = versionSet.getCurrentVersion();
            List<FileMeta> fileMetaList = version.getFileMetaList();
            int level = event.getLevel();
            LOG.info("Handling compaction event: level " + event.getLevel());

            List<FileMeta> upperLevelFiles = null;
            List<FileMeta> lowerLevelFiles = null;

            List<FileMeta> basicFiles = new ArrayList<>();
            for (FileMeta fileMeta : fileMetaList) {
                if (fileMeta.getLevel() == level) {
                    basicFiles.add(fileMeta);
                }
            }
            if (level == 0) {
                if (basicFiles.size() > levelMinSize[0]) {
                    basicFiles.sort((f1, f2) -> {
                        int majorCampare = f1.getMajorId() - f2.getMajorId();
                        if (majorCampare != 0) {
                            return majorCampare;
                        } else {
                            return (int) (f1.getFileId() - f2.getFileId());
                        }
                    });

                    List<FileMeta> level1Files = new ArrayList<>();
                    for (FileMeta fileMeta : fileMetaList) {
                        if (fileMeta.getLevel() == 1) {
                            level1Files.add(fileMeta);
                        }
                    }

                    if (level1Files.size() == 0) {
                        upperLevelFiles = basicFiles;
                        lowerLevelFiles = null;
                    } else {
                        double ra = Double.MAX_VALUE;

                        for (int viceCount = 0; viceCount <= level1Files.size(); viceCount++) {
                            for (int ind = 0; ind <= level1Files.size() - viceCount; ind++) {
                                int leftInd = ind, rightInd = ind + viceCount - 1;
                                byte[] minRowKey = null;
                                byte[] maxRowKey = null;

                                long viceSize = 0L;
                                for (int j = leftInd; j <= rightInd; j++) {
                                    viceSize += level1Files.get(j).getFileSize();
                                }

                                if (leftInd == 0) {
                                    minRowKey = CommonUtils.MIN_BYTE_ARRAY;
                                } else {
                                    minRowKey = level1Files.get(leftInd - 1).getEndRecord().getKey();
                                }

                                if (rightInd == level1Files.size() - 1) {
                                    maxRowKey = CommonUtils.MAX_BYTE_ARRAY;
                                } else {
                                    maxRowKey = level1Files.get(rightInd + 1).getStartRecord().getKey();
                                }

                                List<FileMeta> thisPlan = calcPlan(minRowKey, maxRowKey, basicFiles);
                                if (thisPlan.size() > 0) {
                                    long currentLevelSize = 0L;
                                    for (FileMeta l0Pick : thisPlan) {
                                        currentLevelSize += l0Pick.getFileSize();
                                    }

                                    double thisRA = viceSize * 1.0 / currentLevelSize;
                                    if (lowerLevelFiles == null || thisRA < ra) {
                                        lowerLevelFiles = new ArrayList<>();
                                        for (int j = leftInd; j <= rightInd; j++) {
                                            lowerLevelFiles.add(level1Files.get(j));
                                        }
                                        upperLevelFiles = thisPlan;
                                        ra = thisRA;
                                    }
                                }
                            }
                        }
                    }
                }
            } else if (level < lastLevel) { // 非最后一层需要与下一层进行合并
                // 第1、2层则优先选择尺寸小的文件，减少零碎
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

                if (upperLevelFiles != null) {
                    byte[] leftEnd = CommonUtils.smallestStartKey(upperLevelFiles);
                    byte[] rightEnd = CommonUtils.largestEndKey(upperLevelFiles);

                    //跟据上层文件的记录rk范围选取下层文件
                    lowerLevelFiles = new ArrayList<>();
                    for (FileMeta fileMeta : fileMetaList) {
                        if (CommonUtils.overlapInRange(fileMeta, leftEnd, rightEnd)) {
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

                    for (int i = 0; i <= fileMetaList.size() - targetPickCount; i++) {
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
                // LOG
                // upperLevelFiles， lowerLevelFiles, level
                //TODO 使用Executor进行compaction
            }
        }
        LOG.info("Stopping compaction.");
    }

    private List<FileMeta> calcPlan(byte[] minRowKey, byte[] maxRowKey, List<FileMeta> level0Files) {
        List<FileMeta> ret = new ArrayList<>();
        if (level0Files.size() == 0 || CommonUtils.compareByteArray(minRowKey, maxRowKey) >= 0) return ret;

        long totalSize = 0L;
        byte[] lBorder = minRowKey, rBorder = maxRowKey;

        for (int ind = 0; ind < level0Files.size(); ind++) {

            long currentMajorId = level0Files.get(ind).getMajorId();

            // Process level0Files with same walId and find the segments that can be added
            List<Integer> addedInd = new LinkedList<>();
            while (ind < level0Files.size() && level0Files.get(ind).getMajorId() == currentMajorId) {
                FileMeta l0File = level0Files.get(ind);
                if (CommonUtils.contains(lBorder, rBorder,
                        l0File.getStartRecord().getKey(),
                        l0File.getEndRecord().getKey(),
                        true, true)) {
                    totalSize += l0File.getFileSize();
                    addedInd.add(ind);
                    ret.add(l0File);

                }
                ind++;
            }
            ind--;

            // Update lBorder and rBorder
            if (addedInd.size() > 0) {
                Integer firstAdded = addedInd.get(0);
                Integer lastAdded = addedInd.get(addedInd.size() - 1);

                if (firstAdded > 0) {
                    FileMeta preAdd = level0Files.get(firstAdded - 1);
                    if (preAdd.getMajorId() == currentMajorId) {
                        lBorder = CommonUtils.max(lBorder, preAdd.getEndRecord().getKey());
                    }
                }

                if (lastAdded < level0Files.size() - 1) {
                    FileMeta postAdd = level0Files.get(lastAdded + 1);
                    if (postAdd.getMajorId() == currentMajorId) {
                        rBorder = CommonUtils.min(rBorder, postAdd.getStartRecord().getKey());
                    }
                }

            } else { // No file with this walId is usable.

                // Iterate from right to left
                for (int i = ind; i >= 0 && level0Files.get(i).getMajorId() == currentMajorId; i--) {
                    FileMeta fileMeta = level0Files.get(i);

                    // If there is a segment fully covers the row key range including the border, then the range is narrowed to 0.
                    if (CommonUtils.contains(fileMeta.getStartRecord().getKey(),
                            fileMeta.getEndRecord().getKey(),
                            lBorder, rBorder)) {
                        return ret;
                    }

                    int startKeyCompRBorder = CommonUtils.compareByteArray(fileMeta.getStartRecord().getKey(), rBorder);
                    int startKeyCompLBorder = CommonUtils.compareByteArray(fileMeta.getStartRecord().getKey(), lBorder);
                    int endKeyCompRBorder = CommonUtils.compareByteArray(fileMeta.getEndRecord().getKey(), rBorder);
                    int endKeyCompLBorder = CommonUtils.compareByteArray(fileMeta.getEndRecord().getKey(), lBorder);

                    if (startKeyCompRBorder >= 0 || endKeyCompLBorder <= 0) {
                        // Such segment doesn't overlap with row key range, and can't influence the row key range.
                    } else { // And these ones overlap with (lBorder, rBorder).
                        // If it fully covers the row key range including the border, then the range is narrowed to 0.
                        if (startKeyCompLBorder <= 0 && endKeyCompRBorder >= 0) {
                            return ret;
                        }
                        if (endKeyCompRBorder < 0) { // So startKeyCompLBorder is guaranteed to be <=0, because this segment is not usable.
                            lBorder = fileMeta.getEndRecord().getKey();
                        } else {
                            // startKeyCompLBorder > 0
                            rBorder = fileMeta.getStartRecord().getKey();
                        }
                    }
                }
            }
            if (CommonUtils.compareByteArray(lBorder, rBorder) >= 0) {
                break;
            }
        }

        return ret;
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
        this.running.set(true);
        this.start();
    }

    @Override
    public void stopCompaction() {
        running.set(false);
    }
}
