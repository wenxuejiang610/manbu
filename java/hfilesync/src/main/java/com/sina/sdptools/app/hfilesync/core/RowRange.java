package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

import java.util.List;

public final class RowRange implements Comparable<RowRange> {

    public static final ByteString MIN_START_ROW = ByteString.EMPTY_STRING;
    public static final ByteString MAX_END_ROW = ByteString.EMPTY_STRING;
    public static final RowRange ALL = new RowRange(MIN_START_ROW, MAX_END_ROW);

    private final ByteString startRow; // 空表示最小行
    private final ByteString endRow; // 空表示最大行

    public RowRange(ByteString startRow, ByteString endRow) {
        Preconditions.checkNotNull(startRow);
        Preconditions.checkNotNull(endRow);
        if (!startRow.equals(MIN_START_ROW) &&
                !endRow.equals(MAX_END_ROW) &&
                startRow.compareTo(endRow) >= 0) {
            throw new IllegalArgumentException(
                    "Bad range: " + startRow + " " + endRow);
        }
        this.startRow = startRow;
        this.endRow = endRow;
    }

    public RowRange(ByteString prefix) {
        Preconditions.checkNotNull(prefix);
        if (prefix.equals(MIN_START_ROW) ||
            prefix.equals(MAX_END_ROW) ) {
            throw new IllegalArgumentException(
                    "Bad prefix: " + prefix);
        }
        byte[] tmp = prefix.toByteArray();
        for (int i = tmp.length - 1; i >= 0; --i) {
            int t = tmp[i] & 0xFF;
            if (t != 255) {
                t++;
                tmp[i] = (byte)(t & 0xFF);
                break;
            }
        }
        this.startRow = prefix;
        this.endRow = ByteString.copyFrom(tmp);
    }

    public ByteString getStartRow() {
        return startRow;
    }

    public ByteString getEndRow() {
        return endRow;
    }

    public boolean isStartRowUnbounded() {
        return startRow.equals(MIN_START_ROW);
    }

    public boolean isEndRowUnbounded() {
        return endRow.equals(MAX_END_ROW);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowRange that = (RowRange) o;
        return Objects.equal(this.startRow, that.startRow) && Objects.equal(this.endRow, that.endRow);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(startRow, endRow);
    }

    @Override
    public int compareTo(RowRange that) {
        return ComparisonChain.start()
                .compare(this.startRow, that.startRow)
                .compare(this.endRow, that.endRow)
                .result();
    }

    /** 判断 r1 是否和 r2 中的任何一个相交 */
    public static boolean intersectAny(RowRange r1, List<RowRange> r2) {
        for (RowRange r : r2) {
            if (intersect(r1, r)) {
                return true;
            }
        }
        return false;
    }

    public static boolean intersect(RowRange r1, RowRange r2) {
        if (!r1.isEndRowUnbounded() &&
                !r2.isStartRowUnbounded() &&
                r1.getEndRow().compareTo(r2.getStartRow()) <= 0) {
            return false;
        }
        if (!r2.isEndRowUnbounded() &&
                !r1.isStartRowUnbounded() &&
                r2.getEndRow().compareTo(r1.getStartRow()) <= 0) {
            return false;
        }
        return true;
    }

    /** r1 完全包含 r2 */
    public static boolean contain(RowRange r1, RowRange r2) {
        if ( (r1.isStartRowUnbounded() ||
                (!r2.isStartRowUnbounded() && r1.getStartRow().compareTo(r2.getStartRow()) <= 0)) &&
            (r1.isEndRowUnbounded() ||
                (!r2.isEndRowUnbounded() && r1.getEndRow().compareTo(r2.getEndRow()) >= 0))  ) {
            return true;
        }
        return false;
    }
}
