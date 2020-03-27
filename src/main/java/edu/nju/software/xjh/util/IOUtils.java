package edu.nju.software.xjh.util;

import edu.nju.software.xjh.compaction.CompactionExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;

public class IOUtils {
    public static Logger LOG = LogManager.getLogger(IOUtils.class);

    public static void readFully(InputStream in, byte[] target) throws IOException {
        int readSize = 0;

        while (readSize < target.length) {
            int ret = in.read(target, readSize, target.length - readSize);

            if (ret < 0) {
                throw new IOException("Invalid end of stream");
            }

            readSize += ret;
        }
    }

    public static FileOutputStreamWithMetrics createOutputStream(String filePath) throws FileNotFoundException {
        return new FileOutputStreamWithMetrics(filePath);
    }
    public static void tryDeleteFile(String filePath) {
        try {
            Files.delete(new File(filePath).toPath());
        } catch (IOException e) {
            LOG.info(e.getMessage());
        }
    }
}
