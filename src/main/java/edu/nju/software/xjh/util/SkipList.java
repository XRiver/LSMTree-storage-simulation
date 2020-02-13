package edu.nju.software.xjh.util;

import edu.nju.software.xjh.model.Record;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SkipList {

    private final static int MAX_HEIGHT = 12;
    private final static int BRANCHING = 4;

    private final Random rand;
    private final SkipNode head;
    private int nodeNum;

    private AtomicInteger curMaxHeight;

    public SkipList() {
        head = new SkipNode(null, MAX_HEIGHT);
        rand = new Random();
        curMaxHeight = new AtomicInteger(1);
        nodeNum = 0;

        for (int i = 0; i < MAX_HEIGHT; i++) {
            head.setNext(i, null);
        }
    }

    public boolean isEmpty() {
        return head.getNext(0) == null;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public void reset() {
        nodeNum = 0;
        for (int i = 0; i < MAX_HEIGHT; i++) {
            head.setNext(i, null);
        }
    }

    /**
     * Insert a Record into the SkipList
     */
    public boolean put(Record record) {

        SkipNode[] prev = new SkipNode[MAX_HEIGHT];

        SkipNode prevNode = findGreaterOrEqual(head, curMaxHeight.get() - 1, record, prev);

        if (prevNode != null && prevNode.compareToRowKey(record.getKey()) == 0) {
            Record prevRecord = prevNode.getKey();
            if (prevRecord.getId() <= record.getId()) {
                prevRecord.setValue(record.getValue());
                prevRecord.setId(record.getId());
            }

            return false;
        }

        int randomHeight = randomHeight();

        if (randomHeight > curMaxHeight.get()) {
            for (int i = curMaxHeight.get(); i < randomHeight; i++) {
                prev[i] = head;
            }

            curMaxHeight.set(randomHeight);
        }

        SkipNode newNode = new SkipNode(record, randomHeight);

        for (int i = 0; i < randomHeight; i++) {
            newNode.setNext(i, prev[i].getNext(i));
            prev[i].setNext(i, newNode);
        }

        nodeNum++;

        return true;
    }

    /**
     * Used for reading
     *
     * @param targetKey Target key to looking for
     */
    public boolean contains(byte[] targetKey) {
        if (isEmpty()) {
            return false;
        }

        boolean notInRange = CommonUtils.compareByteArray(getFirstRecord().getKey(), targetKey) > 0 ||
                CommonUtils.compareByteArray(getLastRecord().getKey(), targetKey) < 0;

        if (notInRange) {
            return false;
        }

        SkipNode candidate = findGreaterOrEqual(head, curMaxHeight.get() - 1, targetKey, null);

        //Compare row key and sequence id
        while (candidate != null && candidate.compareToRowKey(targetKey) == 0) {
            return true;
        }

        return false;
    }

    /**
     * Get first record stored in this SkipList, if the SkipList is empty, return null
     */
    public Record getFirstRecord() {
        SkipNode firstNode = head.getNext(0);

        if (firstNode == null) {
            return null;
        }

        return firstNode.getKey();
    }

    /**
     * Get last record stored in this SkipList, if the SkipList is empty, return null
     */
    public Record getLastRecord() {
        SkipNode lastNode = findLast(head, curMaxHeight.get() - 1);

        if (lastNode == null) {
            return null;
        }

        return lastNode.getKey();
    }

    /**
     * For a given rowKey, skip records having smaller rowKey
     */
    private static SkipNode skip(SkipNode tmp, int level, byte[] rowKey) {
        if (rowKey == null) {
            return tmp.getNext(0);
        }

        return findGreaterOrEqual(tmp, level, rowKey, null);
    }

    /**
     * Read a record with a given rowKey and a sequence id
     */
    public Record get(byte[] rowKey) {
        if (isEmpty()) {
            return null;
        }

        boolean notInRange = CommonUtils.compareByteArray(getFirstRecord().getKey(), rowKey) > 0 ||
                CommonUtils.compareByteArray(getLastRecord().getKey(), rowKey) < 0;

        if (notInRange) {
            return null;
        }

        SkipNode candidate = findGreaterOrEqual(head, curMaxHeight.get() - 1, rowKey, null);

        //Compare row key and sequence id
        if (candidate != null && candidate.compareToRowKey(rowKey) == 0) {
            return candidate.getKey();
        }

        return null;
    }

    /**
     * Get an iterator of this SkipList, a start position could be assigned with a user-provided rowKey,
     * records with higher sequence id will not be returned.
     */
    public Iterator<Record> toIterator() {

        return new SkipListIterator(head, curMaxHeight.get() - 1);
    }

    /**
     * Check if current node is smaller than the given row key
     */
    private static boolean isRowKeyAfterNode(byte[] rowKey, SkipNode node) {
        return node != null && node.getKey() != null && node.compareToRowKey(rowKey) < 0;
    }

    /**
     * Find the smallest node whose record is larger than the given row key.
     * If prev is not null, store all pointers of the largest node whose record is smaller than the given row key.
     */
    private static SkipNode findGreaterOrEqual(SkipNode tmp, int level, byte[] rowKey, SkipNode[] prev) {
        while (true) {
            SkipNode next = tmp.getNext(level);

            if (isRowKeyAfterNode(rowKey, next)) {
                tmp = next;
            } else {
                if (prev != null) {
                    prev[level] = tmp;
                }

                if (level == 0) {
                    return next;
                } else {
                    level--;
                }
            }
        }
    }

    /**
     * @param targetRecord The target record
     * @param prev         Previous node link list, can be null
     * @return Node with which row key is larger than the target one
     */
    private static SkipNode findGreaterOrEqual(SkipNode tmp, int level, Record targetRecord, SkipNode[] prev) {
        return findGreaterOrEqual(tmp, level, targetRecord.getKey(), prev);
    }

    /**
     * Find a node whose key is smaller than the given row key
     *
     * @param rowKey Target row key
     * @return Node with which row key is smaller than the target one
     */
    private static SkipNode findLessThan(SkipNode tmpHead, int level, byte[] rowKey) {
        while (true) {
            SkipNode next = tmpHead.getNext(level);

            if (next == null || next.compareToRowKey(rowKey) >= 0) {
                if (level == 0) {
                    return tmpHead;
                } else {
                    level--;
                }
            } else {
                tmpHead = next;
            }
        }
    }

    /**
     * Find the last node
     *
     * @return The last node, null if the list is empty
     */
    private static SkipNode findLast(SkipNode tmp, int level) {
        while (true) {
            SkipNode next = tmp.getNext(level);

            if (next == null) {
                if (level == 0) {
                    return tmp;
                } else {
                    level--;
                }
            } else {
                tmp = next;
            }
        }
    }

    /**
     * Generate a height in probability
     *
     * @return A random height
     */
    private int randomHeight() {
        int height = 1;

        while (height < MAX_HEIGHT && (rand.nextInt() % BRANCHING) == 0) {
            height++;
        }

        return height;
    }

    /**
     * Get an iterator with read sequence id
     */
    private static class SkipListIterator implements Iterator<Record> {

        private final SkipNode headNode;
        private final int level;
        private SkipNode startNode;
        private Record lastRecord;

        public SkipListIterator(SkipNode headNode, int level) {
            this.headNode = headNode;
            this.level = level;
            this.startNode = headNode.getNext(0);
            this.lastRecord = null;
        }

        @Override
        public boolean hasNext() {
            if (lastRecord != null) {
                return true;
            }

            if (startNode != null && startNode.getKey() != null) {
                lastRecord = startNode.getKey();
                startNode = startNode.getNext(0);

                return true;
            }

            return false;
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new IllegalStateException("No more records!");
            }

            Record result = lastRecord;
            lastRecord = null;

            return result;
        }

        @Override
        public void remove() {
        }
    }
}