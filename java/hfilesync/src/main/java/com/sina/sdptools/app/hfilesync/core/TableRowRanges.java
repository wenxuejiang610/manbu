package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Preconditions;

import java.util.List;

/** 表示一个表中的若干个行区间 */
public class TableRowRanges {

    private final String table;
    private final List<RowRange> ranges;

    public TableRowRanges(String table, List<RowRange> ranges) {
        this.table = MorePreconditions.checkNotNullOrEmpty(table);
        this.ranges = Preconditions.checkNotNull(ranges);
    }

    public String getTable() {
        return table;
    }

    public List<RowRange> getRanges() {
        return ranges;
    }
}
