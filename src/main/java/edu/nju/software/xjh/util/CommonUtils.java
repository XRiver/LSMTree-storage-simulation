package edu.nju.software.xjh.util;

import edu.nju.software.xjh.db.FileMeta;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;
import java.util.List;

public class CommonUtils {
    static final Unsafe THE_UNSAFE;
    static long BYTE_ARRAY_BASE_OFFSET;
    static long CHAR_ARRAY_BASE_OFFSET;
    static long BOOLEAN_ARRAY_BASE_OFFSET;
    static long SHORT_ARRAY_BASE_OFFSET;
    static long INT_ARRAY_BASE_OFFSET;
    static long LONG_ARRAY_BASE_OFFSET;
    static long FLOAT_ARRAY_BASE_OFFSET;
    static long DOUBLE_ARRAY_BASE_OFFSET;
    static public final int SIZEOF_LONG = 8;
    static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
    static final long BYTE_BUFFER_HB_FIELD_OFFSET;

    public static final byte[] MIN_BYTE_ARRAY = new byte[0];
    public static final byte[] MAX_BYTE_ARRAY = new byte[1024];

    static {
        for(int i = 0; i < MAX_BYTE_ARRAY.length; i++) {
            MAX_BYTE_ARRAY[i] = -1;
        }

        THE_UNSAFE = (Unsafe) AccessController.doPrivileged(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");
                            f.setAccessible(true);
                            return f.get(null);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            throw new Error();
                        }
                    }
                }
        );

        BYTE_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(byte[].class);
        CHAR_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(char[].class);
        BOOLEAN_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(boolean[].class);
        SHORT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(short[].class);
        INT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(int[].class);
        LONG_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(long[].class);
        FLOAT_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(float[].class);
        DOUBLE_ARRAY_BASE_OFFSET = THE_UNSAFE.arrayBaseOffset(double[].class);

        if (THE_UNSAFE.arrayIndexScale(byte[].class) != 1) {
            throw new AssertionError();
        }

        try {
            BYTE_BUFFER_HB_FIELD_OFFSET = THE_UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compare two byte arrays
     *
     * @param buffer1 Byte array to compare
     * @param buffer2 Byte array to compare
     * @return 0 if equals; > 0 if buffer1 is larger than buffer2; , < 0 if buffer2 is smaller than buffer1
     */
    public static int compareByteArray(byte[] buffer1, byte[] buffer2) {
        return compareByteArray(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
    }

    /**
     * Compare two byte arrays
     *
     * @param buffer1 Byte array to compare
     * @param offset1 Offset
     * @param length1 Length
     * @param buffer2 Byte array to compare
     * @param offset2 Offset
     * @param length2 Length
     * @return 0 if equals; > 0 if buffer1 is larger than buffer2; , < 0 if buffer2 is smaller than buffer1
     */
    public static int compareByteArray(byte[] buffer1, int offset1, int length1,
                                       byte[] buffer2, int offset2, int length2) {

        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
            return 0;
        }

        int minLength = Math.min(length1, length2);
        int minWords = minLength / SIZEOF_LONG;
        long offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
        long offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

        for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
            long lw = THE_UNSAFE.getLong(buffer1, offset1Adj + i);
            long rw = THE_UNSAFE.getLong(buffer2, offset2Adj + i);
            long diff = lw ^ rw;

            if (diff != 0) {
                if (!LITTLE_ENDIAN) {
                    return lessThanUnsigned(lw, rw) ? -1 : 1;
                }

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;
                if (x == 0) {
                    x = (int) (diff >>> 32);
                    n = 32;
                }

                y = x << 16;
                if (y == 0) {
                    n += 16;
                } else {
                    x = y;
                }

                y = x << 8;
                if (y == 0) {
                    n += 8;
                }
                return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
            int a = (buffer1[offset1 + i] & 0xFF);
            int b = (buffer2[offset2 + i] & 0xFF);
            if (a != b) {
                return a - b;
            }
        }

        return length1 - length2;
    }

    /**
     * Compare two ByteBuffer
     *
     * @param buffer1 Byte array to compare
     * @param offset1 Offset
     * @param length1 Length
     * @param buffer2 Byte array to compare
     * @param offset2 Offset
     * @param length2 Length
     * @return 0 if equals; > 0 if buffer1 is larger than buffer2; , < 0 if buffer2 is smaller than buffer1
     */
    public static int compareByteArray(ByteBuffer buffer1, int offset1, int length1,
                                       ByteBuffer buffer2, int offset2, int length2) {

        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
            return 0;
        }

        int minLength = Math.min(length1, length2);
        int minWords = minLength / SIZEOF_LONG;

        for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
            long lw = derLongWithoutMove(buffer1, offset1 + i);
            long rw = derLongWithoutMove(buffer2, offset2 + i);
            long diff = lw ^ rw;

            if (diff != 0) {
                if (!LITTLE_ENDIAN) {
                    return lessThanUnsigned(lw, rw) ? -1 : 1;
                }

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;
                if (x == 0) {
                    x = (int) (diff >>> 32);
                    n = 32;
                }

                y = x << 16;
                if (y == 0) {
                    n += 16;
                } else {
                    x = y;
                }

                y = x << 8;
                if (y == 0) {
                    n += 8;
                }
                return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
            int a = derByteWithoutMove(buffer1, offset1 + i) & 0xFF;
            int b = derByteWithoutMove(buffer2, offset2 + i) & 0xFF;
            if (a != b) {
                return a - b;
            }
        }

        return length1 - length2;
    }


    public static byte derByteWithoutMove(ByteBuffer buffer, int offset) {
        if (buffer.hasArray()) {
            return THE_UNSAFE.getByte(buffer.array(), BYTE_ARRAY_BASE_OFFSET + offset + buffer.arrayOffset());
        } else {
            long address = ((DirectBuffer) buffer).address() + offset;
            return THE_UNSAFE.getByte(address);
        }
    }

    static boolean lessThanUnsigned(long x1, long x2) {
        return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
    }

    public static long derLongWithoutMove(ByteBuffer buffer, int offset) {
        if (buffer.hasArray()) {
            return THE_UNSAFE.getLong(buffer.array(), LONG_ARRAY_BASE_OFFSET + offset + buffer.arrayOffset());
        } else {
            long address = ((DirectBuffer) buffer).address() + offset;
            return THE_UNSAFE.getLong(address);
        }
    }

    public static byte[] min(byte[] b1, byte[] b2) {
        return compareByteArray(b1, b2) < 0 ? b1 : b2;
    }

    public static byte[] max(byte[] b1, byte[] b2) {
        return compareByteArray(b1, b2) > 0 ? b1 : b2;
    }

    public static void sortByStartKey(List<FileMeta> fileMetas) {
        fileMetas.sort((o1, o2) -> compareByteArray(o1.getStartRecord().getKey(), o2.getStartRecord().getKey()));
    }

    public static byte[] smallestStartKey(List<FileMeta> fileMetas) {
        byte[] ret = null;
        for (FileMeta fileMeta : fileMetas) {
            if (ret == null) {
                ret = fileMeta.getStartRecord().getKey();
            } else {
                ret = min(ret, fileMeta.getStartRecord().getKey());
            }
        }
        return ret;
    }

    public static byte[] largestEndKey(List<FileMeta> fileMetas) {
        byte[] ret = null;
        for (FileMeta fileMeta : fileMetas) {
            if (ret == null) {
                ret = fileMeta.getEndRecord().getKey();
            } else {
                ret = max(ret, fileMeta.getEndRecord().getKey());
            }
        }
        return ret;
    }

    public static boolean overlapInRange(FileMeta fileMeta, byte[] lower, byte[] upper) {
        return CommonUtils.compareByteArray(fileMeta.getStartRecord().getKey(), upper) <= 0 ||
                CommonUtils.compareByteArray(fileMeta.getEndRecord().getKey(), lower) >= 0;
    }

    /**
     * A shortcut to check whether rangeX covers rangeY
     * @param xStart
     * @param xEnd
     * @param yStart
     * @param yEnd
     * @return xStart <= yStart && xEnd >= yEnd
     */
    public static boolean contains(byte[] xStart, byte[] xEnd, byte[] yStart, byte[] yEnd) {
        return CommonUtils.compareByteArray(xStart,yStart) <= 0 && CommonUtils.compareByteArray(xEnd, yEnd) >= 0;
    }

    /**
     * A shortcut to check whether rangeX covers rangeY
     * @param xStart
     * @param xEnd
     * @param yStart
     * @param yEnd
     * @param startExclusive if true, condStart is xStart < yStart; otherwise xStart <= yStart
     * @param endExclusive if true, condEnd is xEnd > yEnd; otherwise xEnd >= yEnd
     * @return condStart && condEnd
     */
    public static boolean contains(byte[] xStart, byte[] xEnd, byte[] yStart, byte[] yEnd, boolean startExclusive, boolean endExclusive) {
        int startComp = CommonUtils.compareByteArray(xStart, yStart);
        int endComp = CommonUtils.compareByteArray(xEnd, yEnd);
        boolean condStart = startExclusive ? (startComp < 0):(startComp <= 0);
        boolean condEnd = endExclusive? (endComp > 0):(endComp >= 0);
        return condStart && condEnd;
    }

}
