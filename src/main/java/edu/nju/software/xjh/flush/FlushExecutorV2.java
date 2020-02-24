package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.compaction.CompactionEvent;
import edu.nju.software.xjh.db.*;
import edu.nju.software.xjh.db.event.DBEventType;
import edu.nju.software.xjh.db.metric.MetricEvent;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.model.Segment;
import edu.nju.software.xjh.model.SegmentFactory;
import edu.nju.software.xjh.util.FileOutputStreamWithMetrics;
import edu.nju.software.xjh.util.IOUtils;
import edu.nju.software.xjh.util.SkipList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FlushExecutorV2 extends FlushExecutorV1 {

    public Logger LOG = LogManager.getLogger(FlushExecutorV2.class);

    private final int flushPieces;
    private final AtomicInteger majorFileId;

    public FlushExecutorV2(Config config, Bus bus, VersionSet versionSet, String fileBasePath) {
        super(config, bus, versionSet, fileBasePath);
        flushPieces = Integer.parseInt(config.getVal(Config.ConfigVar.FLUSH_SPLIT_PIECE));
        majorFileId = new AtomicInteger(0);
    }

    @Override
    public void doFlush(SkipList records) {
        if (!busy.compareAndSet(false, true)) {
            LOG.error("Assigned with task while busy!");
            return;
        }

        int majorId = majorFileId.incrementAndGet();

        // 计算每个文件最多可以有多少条记录
        int totalCount = records.getNodeNum();
        int eachFileRecordCount = (totalCount % flushPieces == 0) ?
                totalCount / flushPieces:
                (totalCount + flushPieces - 1) / flushPieces;

        long totalWrittenBytes = 0;
        int createdFileCount = 0;

        long currentSize = 0;
        int currentRecordCount = 0;
        FileMeta fileMeta = null;
        Segment currentSegment = null;
        String filePath = null;

        VersionMod versionMod = new VersionMod(VersionModType.FLUSH);

        Iterator<Record> iterator = records.toIterator();
        FileOutputStreamWithMetrics outputStream = null;
        Record record = null;
        try {

            while (iterator.hasNext()) {
                record = iterator.next();

                if (currentSegment == null) {
                    long fileId = versionSet.getNextFileId();
                    filePath = fileBasePath + "/" + majorId + "-"+ fileId + ".data";
                    LOG.info("Allocating file path:" + filePath);

                    currentSegment = SegmentFactory.createEmpty();
                    createdFileCount++;
                    fileMeta = new FileMeta(fileId, filePath);
                    fileMeta.setStartRecord(record);
                }

                currentSize += record.getApproximateLength();
                currentRecordCount += 1;
                currentSegment.addRecord(record);

                // 每一个文件还按照记录数进行限制，保障一个SkipList能产生均匀的多个文件
                if (currentSize >= fileSizeLimit || currentRecordCount == eachFileRecordCount) {
                    outputStream = IOUtils.createOutputStream(filePath);
                    currentSegment.writeTo(outputStream);
                    outputStream.close();
                    totalWrittenBytes += outputStream.getWrittenBytes();

                    fileMeta.setLevel(0);
                    fileMeta.setFileSize(currentSegment.getSize());
                    fileMeta.setEndRecord(record);
                    fileMeta.setRecordNumber(currentRecordCount);
                    // 为了辨识来自同一批flush的多个L0文件，需要添加这个ID；ID相同的为同期flush的
                    fileMeta.setMajorId(majorId);
                    versionMod.addFile(fileMeta);

                    LOG.info("Flushed new file:" + fileMeta);

                    currentSize = 0;
                    currentRecordCount = 0;
                    currentSegment = null;
                    fileMeta = null;
                }
            }

            if (currentSegment != null) {
                outputStream = IOUtils.createOutputStream(filePath);
                currentSegment.writeTo(outputStream);
                outputStream.close();
                totalWrittenBytes += outputStream.getWrittenBytes();

                fileMeta.setLevel(0);
                fileMeta.setFileSize(currentSegment.getSize());
                fileMeta.setEndRecord(record);
                fileMeta.setRecordNumber(currentRecordCount);
                fileMeta.setMajorId(majorId);
                versionMod.addFile(fileMeta);

                LOG.info("Flushed new file:" + fileMeta);
            }

            records.reset();

            if (createdFileCount > 0) {
                versionSet.applyNewVersion(versionMod);

                bus.push(new CompactionEvent(0));

                MetricEvent metricEvent = new MetricEvent(DBEventType.FLUSH, 0);
                metricEvent.setCreatedFiles(createdFileCount);
                metricEvent.setCreatedBytes(totalWrittenBytes);
                bus.push(metricEvent);

                LOG.info("Flush complete. "+metricEvent);
            } else {
                LOG.info("Flushed an empty SkipList. Not creating new files...");
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        } finally {
            busy.set(false);
        }
    }
}
