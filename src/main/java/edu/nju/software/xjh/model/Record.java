package edu.nju.software.xjh.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Record {
    void writeTo(OutputStream os) throws IOException;
    void readFrom(InputStream is) throws IOException;
    byte[] getKey();
    void setKey(byte[] key);
    byte[] getValue();
    void setValue(byte[] value);
    int getApproximateLength();
    void setId(long id);
    long getId();
}
