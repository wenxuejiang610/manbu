package com.sina.sdptools.app.hfilesync.core;

import com.google.common.collect.ImmutableList;
import com.sina.sdptools.app.hfilesync.core.ByteString;
import com.sina.sdptools.app.hfilesync.core.RowRange;
import com.sina.sdptools.app.hfilesync.util.JobNamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

import static com.sina.sdptools.app.hfilesync.core.TestingUtils.*;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class RowRangeTest {

    @Test
    public void testRowRange() throws IOException, InterruptedException {
        RowRange all = RowRange.ALL;
        RowRange left_all = new RowRange(RowRange.MIN_START_ROW, ByteString.copyFrom("cydu".getBytes()));
        RowRange right_all = new RowRange(ByteString.copyFrom("cydu".getBytes()), RowRange.MAX_END_ROW);
        RowRange normal = new RowRange(ByteString.copyFrom("aaaa".getBytes()), ByteString.copyFrom("abcd".getBytes()));
        RowRange normal2 = new RowRange(ByteString.copyFrom("aabb".getBytes()), ByteString.copyFrom("cydu".getBytes()));
        RowRange normal3 = new RowRange(ByteString.copyFrom("aa".getBytes()), ByteString.copyFrom("cydz".getBytes()));
        RowRange prefix = new RowRange(ByteString.copyFrom("aaaaa".getBytes()));

        Assert.isTrue(RowRange.contain(all, all));
        Assert.isTrue(RowRange.contain(all, left_all));
        Assert.isTrue(RowRange.contain(all, right_all));
        Assert.isTrue(RowRange.contain(all, normal));

        Assert.isTrue(!RowRange.contain(normal, all));
        Assert.isTrue(!RowRange.contain(right_all, all));
        Assert.isTrue(!RowRange.contain(left_all, all));

        Assert.isTrue(RowRange.contain(left_all, normal));
        Assert.isTrue(!RowRange.contain(right_all, normal));

        Assert.isTrue(RowRange.contain(normal, normal));
        Assert.isTrue(!RowRange.contain(normal, normal2));
        Assert.isTrue(!RowRange.contain(normal, normal3));
        Assert.isTrue(RowRange.contain(normal3, normal));
        Assert.isTrue(RowRange.contain(normal3, normal2));

        Assert.isTrue(RowRange.contain(prefix, prefix));
        Assert.isTrue(RowRange.contain(normal3, prefix));
        Assert.isTrue(!RowRange.contain(prefix, normal3 ));
        Assert.isTrue(RowRange.contain(left_all, prefix));
        Assert.isTrue(!RowRange.contain(prefix, left_all));
    }
}
