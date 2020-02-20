package edu.nju.software.xjh.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * 一个存储多条记录的数据文件。可能处于“只读”状态或“可写”状态。两个状态只会单向地从可写变为只读。
 */
public interface Segment {
    /**
     * @return 当前Segment是否还可写。
     */
    boolean isWritable();

    /**
     * 按顺序取得Segment的所有Record。仅可在“只读”状态下调用。
     * @return Record的迭代器
     */
    Iterator<Record> getRecords();

    /**
     * 在文件末尾加入一条记录。仅可在“可写”状态下调用。
     * @param record
     */
    void addRecord(Record record);

    /**
     * 将Segment序列化写到输出流。仅可在“可写”状态下调用，并且会把Segment状态转变成“只读”。
     * @param os
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * 获取第一条记录的key。仅可在“只读”状态下调用。
     * @return 若没有记录，则会返回null
     */
    byte[] getStartKey();

    /**
     * 获取最后一条记录的key。仅可在“只读”状态下调用。
     * @return 若没有记录，则会返回null
     */
    byte[] getEndKey();

    /**
     * 获取文件大小（字节数）。仅可在“只读”状态下调用。
     * @return
     */
    long getSize();
}
