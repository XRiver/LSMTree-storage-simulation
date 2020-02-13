package edu.nju.software.xjh.util;

import edu.nju.software.xjh.model.Record;

public class SkipNode {
    private Record key;
    private SkipNode[] skipTable;

    public SkipNode(Record key, int height) {
        this.key = key;
        this.skipTable = new SkipNode[height];
    }

    public Record getKey() {
        return key;
    }

    public void setKey(Record record) {
        key = record;
    }

    public int compareToRowKey(byte[] rowKey) {
        return CommonUtils.compareByteArray(key.getKey(), rowKey);
    }

    public SkipNode getNext(int n) {
        assert n < skipTable.length : "Index in SkipNode is out of bound";
        return skipTable[n];
    }

    public void setNext(int n, SkipNode node) {
        if (n >= skipTable.length) {
            expandNextPointerArray(n + 1);
        }

        skipTable[n] = node;
    }

    private void expandNextPointerArray(int height) {
        SkipNode[] newArr = new SkipNode[height];

        System.arraycopy(skipTable, 0, newArr, 0, skipTable.length);
        skipTable = newArr;
    }
}
