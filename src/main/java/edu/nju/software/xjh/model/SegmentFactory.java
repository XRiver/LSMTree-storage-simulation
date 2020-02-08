package edu.nju.software.xjh.model;

import java.io.IOException;
import java.io.InputStream;

public class SegmentFactory {
    /**
     * 从文件输入流获取一个只读的Segment
     * @param is
     * @return
     */
    public static Segment createFrom(InputStream is) throws IOException {
        return SegmentImpl.createFrom(is);
    }

    public static Segment createEmpty() {
        return new SegmentImpl();
    }
}
