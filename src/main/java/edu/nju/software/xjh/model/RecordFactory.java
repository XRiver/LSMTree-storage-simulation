package edu.nju.software.xjh.model;

import java.io.IOException;
import java.io.InputStream;

public class RecordFactory {
    public static Record readFrom(InputStream is) throws IOException {
        Record ret = new RecordImpl();
        ret.readFrom(is);
        return ret;
    }
}
