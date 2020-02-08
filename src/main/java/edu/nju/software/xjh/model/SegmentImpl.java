package edu.nju.software.xjh.model;

import edu.nju.software.xjh.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;

class SegmentImpl implements Segment {
    private static final int MAGIC = 0x12345678;

    private boolean isWritable = true;
    private LinkedList<Record> records = new LinkedList<Record>();

    public boolean isWritable() {
        return isWritable;
    }

    public Iterator<Record> getRecords() {
        if (!isWritable) {
            return records.iterator();
        } else {
            throw new IllegalStateException("This segment is under writing.");
        }
    }

    public void addRecord(Record record) {
        if (isWritable) {
            records.add(record);
        } else {
            throw new IllegalStateException("This segment is read-only.");
        }
    }

    public void writeTo(OutputStream os) throws IOException {
        if (isWritable) {
            os.write(VarInt.encodeSignedVarInt(MAGIC));
            os.write(VarInt.encodeSignedVarInt(records.size()));
            for (Record record : records) {
                record.writeTo(os);
            }
            isWritable = false;
        } else {
            throw new IllegalStateException("This segment is read-only.");
        }
    }

    public byte[] getStartKey() {
        if (!isWritable && records.size() > 0) {
            return records.getFirst().getKey();
        } else {
            return null;
        }
    }

    public byte[] getEndKey() {
        if (!isWritable && records.size() > 0) {
            return records.getLast().getKey();
        } else {
            return null;
        }
    }

    public static Segment createFrom(InputStream is) throws IOException {
        int magic = VarInt.decodeSignedVarInt(is);
        if (magic != MAGIC) {
            throw new IOException("Corrupted data: magic word doesn't match");
        }

        int size = VarInt.decodeSignedVarInt(is);
        SegmentImpl ret = new SegmentImpl();
        for (int i = 0; i < size; i++) {
            ret.addRecord(RecordFactory.readFrom(is));
        }
        ret.isWritable = false;

        return ret;
    }
}
