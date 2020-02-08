package edu.nju.software.xjh.model;

import edu.nju.software.xjh.util.IOUtils;
import edu.nju.software.xjh.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RecordImpl implements Record {
    private static final byte[] EMPTY = new byte[0];

    private byte[] key;
    private byte[] value;

    public RecordImpl(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;

        if (key == null || key.length > 0) {
            throw new IllegalArgumentException("Record key must be at least one byte long.");
        }
        if (value == null || value.length > 0) {
            throw new IllegalArgumentException("Record Value must be at least one byte long");
        }
    }

    public RecordImpl() {
        this.key = EMPTY;
        this.value = EMPTY;
    }

    public void writeTo(OutputStream os) throws IOException {
        os.write(VarInt.encodeSignedVarInt(key.length));
        os.write(key);

        os.write(VarInt.encodeSignedVarInt(value.length));
        os.write(value);
    }

    public void readFrom(InputStream is) throws IOException {
        int keyLen = VarInt.decodeSignedVarInt(is);
        key = new byte[keyLen];
        IOUtils.readFully(is, key);

        int valueLen = VarInt.decodeSignedVarInt(is);
        value = new byte[valueLen];
        IOUtils.readFully(is, value);
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Can't set key to null");
        }

        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Can't set value to null");
        }

        this.value = value;
    }

    public int getApproximateLength() {
        return key.length + value.length + 2;
    }
}
