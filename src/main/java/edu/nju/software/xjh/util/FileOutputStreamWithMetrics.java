package edu.nju.software.xjh.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FileOutputStreamWithMetrics extends OutputStream {

    private FileOutputStream inner;
    private long byteCount;

    public FileOutputStreamWithMetrics(String filePath) throws FileNotFoundException {
        inner = new FileOutputStream(filePath);
        byteCount = 0;
    }

    @Override
    public void write(int b) throws IOException {
        inner.write(b);
        byteCount ++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        inner.write(bytes, offset, length);
        byteCount += length;
    }
}
