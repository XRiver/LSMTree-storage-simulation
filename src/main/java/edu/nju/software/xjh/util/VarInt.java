package edu.nju.software.xjh.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>Encodes signed and unsigned values using a common variable-length
 * scheme, found for example in
 * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
 * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
 * but will use slightly more bytes to encode large values.</p>
 * <p/>
 * <p>Signed values are further encoded using so-called zig-zag encoding
 * in order to make them "compatible" with variable-length encoding.</p>
 */
public class VarInt {
    private VarInt() {

    }

    /**
     * Encode a signed long to varlong
     *
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently
     * encode signed values. If values are known to be nonnegative,
     * {@link #encodeUnsignedVarLong(long)} should be used.
     *
     * @param value long value
     * @return varlong in byte[]
     */
    public static byte[] encodeSignedVarLong(long value) {
        return encodeUnsignedVarLong((value << 1) ^ (value >> 63));
    }

    /**
     * Encode an unsigned long to varlong
     *
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
     * If values can be negative, use {@link #encodeSignedVarLong(long)}
     * instead. This method treats negative input as like a large unsigned value.
     *
     * @param value long value
     * @return varlong in byte[]
     */
    public static byte[] encodeUnsignedVarLong(long value) {
        byte[] byteArrayList = new byte[10];
        int i = 0;

        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            byteArrayList[i++] = (byte) (((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }

        byteArrayList[i] = (byte) ((int) value & 0x7F);

        byte[] out = new byte[i + 1];

        for (; i >= 0; i--) {
            out[i] = byteArrayList[i];
        }

        return out;
    }

    /**
     * Encode a signed int to varint
     *
     * @param value int value
     * @return varint in byte[]
     */
    public static byte[] encodeSignedVarInt(int value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        return encodeUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    public static int getSignedVarIntLength(int value) {
        return getUnsignedVarIntLength((value << 1) ^ (value >> 31));
    }

    /**
     * Encode an unsigned int to varint from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
     * This method treats negative input as like a large unsigned value.
     *
     * @param value int value
     * @return varint in byte[]
     */
    public static byte[] encodeUnsignedVarInt(int value) {
        byte[] byteArrayList = new byte[10];
        int i = 0;

        while ((value & 0xFFFFFF80) != 0L) {
            byteArrayList[i++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }

        byteArrayList[i] = (byte) (value & 0x7F);

        byte[] out = new byte[i + 1];

        for (; i >= 0; i--) {
            out[i] = byteArrayList[i];
        }

        return out;
    }

    public static int getUnsignedVarIntLength(int value) {
        int i = 0;

        while ((value & 0xFFFFFF80) != 0L) {
            i++;
            value >>>= 7;
        }
        return i + 1;
    }

    /**
     * Decode signed varlong
     *
     * @param buffer buffer
     * @param offset offset
     * @return long value
     * @throws IOException exception
     */
    public static long decodeSignedVarLong(byte[] buffer, int offset) throws IOException {
        long raw = decodeUnsignedVarLong(buffer, offset);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * Decode signed varlong
     *
     * @param buffer buffer
     * @return long value
     */
    public static long decodeSignedVarLong(ByteBuffer buffer) {
        long raw = decodeUnsignedVarLong(buffer);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * Decode unsigned varlong
     *
     * @param buffer buffer
     * @param offset offset
     * @return long value
     */
    public static long decodeUnsignedVarLong(byte[] buffer, int offset) {
        long value = 0L;
        int i = 0;
        long b;

        for (int pos = offset; pos < buffer.length; pos++) {
            b = buffer[pos];

            if ((b & 0x80L) == 0) {
                return value | (b << i);
            }

            value |= (b & 0x7F) << i;
            i += 7;

            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }

        throw new IllegalArgumentException("Invalid buffer");
    }

    /**
     * Decode unsigned varlong
     *
     * @param buffer buffer
     * @return long value
     */
    public static long decodeUnsignedVarLong(ByteBuffer buffer) {
        long value = 0L;
        int i = 0;
        long b;

        while (((b = buffer.get()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }

        return value | (b << i);
    }

    /**
     * Decode a varint to a signed int
     *
     * @param buffer buffer
     * @param offset offset
     * @return int
     */
    public static int decodeSignedVarInt(byte[] buffer, int offset) {
        int raw = decodeUnsignedVarInt(buffer, offset);

        int temp = (((raw << 31) >> 31) ^ raw) >> 1;

        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * Decode a varint to a signed int
     *
     * @param buffer buffer
     * @return int
     */
    public static int decodeSignedVarInt(ByteBuffer buffer) {
        int raw = decodeUnsignedVarInt(buffer);

        int temp = (((raw << 31) >> 31) ^ raw) >> 1;

        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * Decode a varint to a signed int from an InputStream
     *
     * @param is
     * @return int
     */
    public static int decodeSignedVarInt(InputStream is) throws IOException {
        int raw = decodeUnsignedVarInt(is);

        int temp = (((raw << 31) >> 31) ^ raw) >> 1;

        return temp ^ (raw & (1 << 31));
    }

    /**
     * Decode a varint to an unsigned normal int
     *
     * @param buffer buffer
     * @param offset offset
     * @return normal int
     */
    public static int decodeUnsignedVarInt(byte[] buffer, int offset) {
        int value = 0;
        int i = 0;
        int b;

        for (int pos = offset; pos < buffer.length; pos++) {
            b = buffer[pos];

            if ((b & 0x80) == 0) {
                return value | (b << i);
            }

            value |= (b & 0x7F) << i;
            i += 7;

            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }

        throw new IllegalArgumentException("Invalid buffer");
    }

    /**
     * Decode a varint to an unsigned int
     *
     * @param buffer buffer
     * @return int
     */
    public static int decodeUnsignedVarInt(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;

        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }

        return value | (b << i);
    }

    /**
     * Decode a varint to an unsigned int from an InputStream
     *
     * @param is
     * @return int
     */
    public static int decodeUnsignedVarInt(InputStream is) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while (((b = is.read()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }

        return value | (b << i);
    }
}
