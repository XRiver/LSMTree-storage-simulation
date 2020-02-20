package edu.nju.software.xjh.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtils {

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

    public static OutputStream createOutputStream(String filePath) throws FileNotFoundException {
        return new FileOutputStreamWithMetrics(filePath);
    }
}
