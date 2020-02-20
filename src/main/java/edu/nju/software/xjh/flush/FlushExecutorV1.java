package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.*;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.model.Segment;
import edu.nju.software.xjh.model.SegmentFactory;
import edu.nju.software.xjh.util.IOUtils;
import edu.nju.software.xjh.util.SkipList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 设计为于FlushHandler同步的模式.
 */
public class FlushExecutorV1 {

    public Logger LOG = LogManager.getLogger(FlushExecutorV1.class);

    private AtomicBoolean busy = new AtomicBoolean(false);

    private final long fileSizeLimit;
    private final String fileBasePath;
    private final VersionSet versionSet;

    public FlushExecutorV1(Config config, VersionSet versionSet, String fileBasePath) {
        this.versionSet = versionSet;
        this.fileBasePath = fileBasePath;
        this.fileSizeLimit =
                Long.parseLong(config.getVal(Config.ConfigVar.SEGMENT_MAX_SIZE).split(",")[0]) * 1024 * 1024;
    }

    public void doFlush(SkipList records) {
        if (!busy.compareAndSet(false, true)) {
            LOG.error("Assigned with task while busy!");
            return;
        }

        long currentSize = 0;
        int currentRecordCount = 0;
        FileMeta fileMeta = null;
        Segment currentSegment = null;
        String filePath = null;

        VersionMod versionMod = new VersionMod(VersionModType.FLUSH);

        Iterator<Record> iterator = records.toIterator();
        try {

            while (iterator.hasNext()) {
                Record record = iterator.next();

                if (currentSegment == null) {
                    long fileId = versionSet.getNextFileId();
                    filePath = fileBasePath + "/" + fileId + ".data";

                    currentSegment = SegmentFactory.createEmpty();
                    fileMeta = new FileMeta(fileId, filePath);
                    fileMeta.setStartRecord(record);
                }

                currentSize += record.getApproximateLength();
                currentRecordCount += 1;
                currentSegment.addRecord(record);


                if (currentSize >= fileSizeLimit) {
                    OutputStream outputStream = IOUtils.createOutputStream(filePath);
                    currentSegment.writeTo(outputStream);

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

            records.reset();

            versionSet.applyNewVersion(versionMod);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            busy.set(false);
        }
    }

    public boolean isBusy() {
        return busy.get();
    }
}
