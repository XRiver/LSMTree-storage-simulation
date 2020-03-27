package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.*;
import edu.nju.software.xjh.db.event.DBEventType;
import edu.nju.software.xjh.db.metric.MetricEvent;
import edu.nju.software.xjh.model.FileMeta;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.model.Segment;
import edu.nju.software.xjh.model.SegmentFactory;
import edu.nju.software.xjh.util.CommonUtils;
import edu.nju.software.xjh.util.FileOutputStreamWithMetrics;
import edu.nju.software.xjh.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 进行compaction；向VersionSet更新状态；向Bus发送统计信息
 */
public class CompactionExecutor {
    public Logger LOG = LogManager.getLogger(CompactionExecutor.class);

    private int lastLevel;
    private String fileDir;
    private VersionSet versionSet;
    private Bus bus;
    private long[] maxSize;

    public CompactionExecutor(Config config, VersionSet versionSet, Bus bus) {
        this.lastLevel = Integer.parseInt(config.getVal(Config.ConfigVar.LEVEL_COUNT)) - 1;
        this.fileDir = config.getVal(Config.ConfigVar.FILE_BASE_PATH);
        this.versionSet = versionSet;
        this.bus = bus;

        String[] split = config.getVal(Config.ConfigVar.SEGMENT_MAX_SIZE).split(",");
        this.maxSize = new long[split.length];
        for (int i = 0; i < split.length; i++) {
            maxSize[i] = Long.parseLong(split[i]) * 1024 * 1024; // MB to Byte
        }
    }

    public void doCompactionV1(List<FileMeta> upperLevelFiles, List<FileMeta> lowerLevelFiles, int level) {
        int targetLevel = level == lastLevel ? level : level + 1;
        long fileSizeLimit = maxSize[targetLevel];

        Iterator<Record> it = level == lastLevel ?
                new NopRecordMerger(upperLevelFiles) :
                new RecordMerger(upperLevelFiles, lowerLevelFiles);

        int totalReadCount = upperLevelFiles.size() + lowerLevelFiles.size();
        long totalReadBytes = 0;
        for (FileMeta upperLevelFile : upperLevelFiles) {
            totalReadBytes += upperLevelFile.getFileSize();
        }
        for (FileMeta lowerLevelFile : lowerLevelFiles) {
            totalReadBytes += lowerLevelFile.getFileSize();
        }

        int totalWriteCount = 0;
        long totalWriteBytes = 0;
        List<FileMeta> addedFiles = new ArrayList<>();

        long currentSize = 0;
        int currentCount = 0;
        Segment currentSegment = null;
        FileMeta currentFileMeta = null;

        Record rec = it.next();
        while (it.hasNext()) {
            Record next = it.next();

            if (CommonUtils.compareByteArray(rec.getKey(), next.getKey()) == 0) {
                rec = next;
                continue;
            }

            if (currentSegment == null) {
                currentSegment = SegmentFactory.createEmpty();

                long fileId = versionSet.getNextFileId();
                String segmentPath = fileDir + "/" + fileId + ".data";
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Allocated fileId " + fileId);
                }
                currentFileMeta = new FileMeta(fileId, segmentPath);
                currentFileMeta.setStartRecord(rec);
                currentFileMeta.setLevel(targetLevel);
            }
            currentSize += rec.getApproximateLength();
            currentSegment.addRecord(rec);
            currentFileMeta.setEndRecord(rec);
            currentCount += 1;

            if (currentSize >= fileSizeLimit) {
                String segmentPath = fileDir + "/" + currentFileMeta.getFileId() + ".data";
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Compaction writing to segment path = " + segmentPath);
                }

                try {
                    FileOutputStreamWithMetrics outputStream = IOUtils.createOutputStream(segmentPath);
                    currentSegment.writeTo(outputStream);

                    outputStream.close();
                    totalWriteCount += 1;
                    totalWriteBytes += outputStream.getWrittenBytes();

                    currentFileMeta.setRecordNumber(currentCount);
                    currentFileMeta.setFileSize(currentSize);
                    addedFiles.add(currentFileMeta);

                    currentSegment = null;
                    currentCount = 0;
                    currentSize = 0;
                    currentFileMeta = null;
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            rec = next;
        }

        if (currentSegment != null) {
            String segmentPath = fileDir + "/" + currentFileMeta.getFileId() + ".data";
            LOG.info("Compaction writing to segment path = " + segmentPath);

            try {
                FileOutputStreamWithMetrics outputStream = IOUtils.createOutputStream(segmentPath);
                currentSegment.writeTo(outputStream);

                outputStream.close();
                totalWriteCount += 1;
                totalWriteBytes += outputStream.getWrittenBytes();

                currentFileMeta.setRecordNumber(currentCount);
                currentFileMeta.setFileSize(currentSize);
                addedFiles.add(currentFileMeta);
            } catch (IOException e) {
                LOG.error(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        long upperConsumedBytes = 0;
        long lowerConsumedBytes = 0;

        VersionMod mod = new VersionMod(VersionModType.COMPACTION);
        for (FileMeta upperLevelFile : upperLevelFiles) {
            mod.removeFile(upperLevelFile);
            upperConsumedBytes += upperLevelFile.getFileSize();
            IOUtils.tryDeleteFile(upperLevelFile.getFilePath());
        }
        for (FileMeta lowerLevelFile : lowerLevelFiles) {
            mod.removeFile(lowerLevelFile);
            lowerConsumedBytes += lowerLevelFile.getFileSize();
            IOUtils.tryDeleteFile(lowerLevelFile.getFilePath());
        }
        for (FileMeta addedFile : addedFiles) {
            mod.addFile(addedFile);
        }

        versionSet.applyNewVersion(mod);

        MetricEvent metricEvent = new MetricEvent(DBEventType.COMPACTION, targetLevel);
        metricEvent.setCreatedBytes(totalWriteBytes);
        metricEvent.setCreatedFiles(totalWriteCount);
        metricEvent.setDeletedBytes(totalReadBytes);
        metricEvent.setDeletedFiles(totalReadCount);
        metricEvent.setUpperConsumedBytes(upperConsumedBytes);
        metricEvent.setLowerConsumedBytes(lowerConsumedBytes);

//        if (targetLevel == 1) {
//            LOG.warn("upper count " + upperLevelFiles.size() + " lower count " + lowerLevelFiles.size());
//            LOG.warn(mod);
//            LOG.warn(metricEvent);
//        }

        bus.push(metricEvent);
        bus.push(new CompactionEvent(targetLevel));
    }

    public void doCompactionV2(List<FileMeta> upperLevelFiles, List<FileMeta> lowerLevelFiles, int level) {
        doCompactionV1(upperLevelFiles, lowerLevelFiles, level);
    }

    class RecordMerger implements Iterator<Record> {
        private OpRecordMerger m1;
        private NopRecordMerger m2;
        private Record r1 = null;
        private Record r2 = null;

        public RecordMerger(List<FileMeta> upper, List<FileMeta> lower) {
            m1 = new OpRecordMerger(upper);
            m2 = new NopRecordMerger(lower);

            if (m1.hasNext()) {
                r1 = m1.next();
            }
            if (m2.hasNext()) {
                r2 = m2.next();
            }
        }

        public boolean hasNext() {
            return !(r1 == null && r2 == null);
        }

        public Record next() {
            Record ret = null;
            if (r1 == null) {
                ret = r2;
                if (m2.hasNext()) {
                    r2 = m2.next();
                } else {
                    r2 = null;
                }
            } else if (r2 == null) {
                ret = r1;
                if (m1.hasNext()) {
                    r1 = m1.next();
                } else {
                    r1 = null;
                }
            } else if (CommonUtils.compareByteArray(r1.getKey(), r2.getKey()) < 0) {
                ret = r1;
                if (m1.hasNext()) {
                    r1 = m1.next();
                } else {
                    r1 = null;
                }
            } else {
                ret = r2;
                if (m2.hasNext()) {
                    r2 = m2.next();
                } else {
                    r2 = null;
                }
            }
            return ret;
        }
    }

    class OpRecordMerger implements Iterator<Record> {

        Iterator<Record> records;

        public OpRecordMerger(List<FileMeta> files) {
            List<Record> recordList = new ArrayList<>();
            for (FileMeta file : files) {
                try {
                    Segment read = SegmentFactory.createFrom(new FileInputStream(file.getFilePath()));
                    Iterator<Record> recordIterator = read.getRecords();
                    while (recordIterator.hasNext()) {
                        recordList.add(recordIterator.next());
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            recordList.sort((o1, o2) -> CommonUtils.compareByteArray(o1.getKey(), o2.getKey()));
            records = recordList.iterator();
        }

        public boolean hasNext() {
            return records.hasNext();
        }

        public Record next() {
            return records.next();
        }
    }

    class NopRecordMerger implements Iterator<Record> {

        private Iterator<FileMeta> fileMetaIterator;
        private Iterator<Record> recordIterator = null;

        public NopRecordMerger(List<FileMeta> files) {
            fileMetaIterator = files.iterator();
        }

        public boolean hasNext() {
            return (recordIterator != null && recordIterator.hasNext()) || fileMetaIterator.hasNext();
        }

        public Record next() {

            if (recordIterator == null || !recordIterator.hasNext()) {
                if (fileMetaIterator.hasNext()) {
                    try {
                        Segment read = SegmentFactory.createFrom(new FileInputStream(fileMetaIterator.next().getFilePath()));
                        recordIterator = read.getRecords();
                    } catch (IOException e) {
                        LOG.error(e.getMessage());
                        throw new RuntimeException(e);
                    }
                }
            }

            if (recordIterator != null && recordIterator.hasNext()) {
                return recordIterator.next();
            } else {
                return null;
            }

        }
    }
}
