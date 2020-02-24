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

/**
 * 设计为于FlushHandler同步的模式.
 */
public class FlushExecutorV1 implements FlushExecutor {

    public Logger LOG = LogManager.getLogger(FlushExecutorV1.class);

    protected AtomicBoolean busy = new AtomicBoolean(false);

    protected final long fileSizeLimit;
    protected final String fileBasePath;
    protected final VersionSet versionSet;
    protected final Bus bus;

    public FlushExecutorV1(Config config, Bus bus, VersionSet versionSet, String fileBasePath) {
        this.versionSet = versionSet;
        this.bus = bus;
        this.fileBasePath = fileBasePath;
        this.fileSizeLimit =
                Long.parseLong(config.getVal(Config.ConfigVar.SEGMENT_MAX_SIZE).split(",")[0]) * 1024 * 1024;
    }

    public void doFlush(SkipList records) {
        if (!busy.compareAndSet(false, true)) {
            LOG.error("Assigned with task while busy!");
            return;
        }

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
                    filePath = fileBasePath + "/" + fileId + ".data";
                    LOG.info("Allocating file path:" + filePath);

                    currentSegment = SegmentFactory.createEmpty();
                    createdFileCount++;
                    fileMeta = new FileMeta(fileId, filePath);
                    fileMeta.setStartRecord(record);
                }

                currentSize += record.getApproximateLength();
                currentRecordCount += 1;
                currentSegment.addRecord(record);


                if (currentSize >= fileSizeLimit) {
                    outputStream = IOUtils.createOutputStream(filePath);
                    currentSegment.writeTo(outputStream);
                    outputStream.close();
                    totalWrittenBytes += outputStream.getWrittenBytes();

                    fileMeta.setLevel(0);
                    fileMeta.setFileSize(currentSegment.getSize());
                    fileMeta.setEndRecord(record);
                    fileMeta.setRecordNumber(currentRecordCount);
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

    public boolean isBusy() {
        return busy.get();
    }
}
