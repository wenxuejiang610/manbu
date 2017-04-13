package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;

/**
 * 封装一段 byte 数组，类似 {@code protobuf} 中的 {@code ByteString}。
 *
 * 这个类实现 {@link java.lang.Comparable} 接口，便于放入 {@code Map} 中比较。
 */
public final class ByteString implements Comparable<ByteString> {

    public static final ByteString EMPTY_STRING = new ByteString(new byte[0]);

    private final byte[] bytes;

    private ByteString(byte[] bytes) {
        this.bytes = Preconditions.checkNotNull(bytes);
    }

    public static ByteString copyFrom(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        return new ByteString(Arrays.copyOf(bytes, bytes.length));
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public int compareTo(ByteString that) {
        for (int i = 0; i < this.bytes.length && i < that.bytes.length; i++) {
            int a = this.bytes[i] & 0xFF;
            int b = that.bytes[i] & 0xFF;
            if (a != b) {
                return a < b ? -1 : 1;
            }
        }
        return Ints.compare(this.bytes.length, that.bytes.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteString that = (ByteString) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (byte b : bytes) {
            s.append((char) b);
        }
        return s.toString();
    }
}
