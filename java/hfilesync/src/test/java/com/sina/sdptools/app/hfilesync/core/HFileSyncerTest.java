package com.sina.sdptools.app.hfilesync.core;

import com.google.common.collect.ImmutableList;
import com.sina.sdptools.app.hfilesync.core.HFileSyncer;
import com.sina.sdptools.app.hfilesync.core.RowRange;
import com.sina.sdptools.app.hfilesync.core.TableRowRanges;
import com.sina.sdptools.app.hfilesync.util.JobNamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

import static com.sina.sdptools.app.hfilesync.core.TestingUtils.assertFileContent;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.createFile;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.randomHFileOrRegionName;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.readResourceAsBytes;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class HFileSyncerTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    private Configuration conf;
    private FileSystem fs;
    private Path hbaseRootDir;
    private Path targetDir;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        fs = FileSystem.getLocal(conf);
        hbaseRootDir = new Path(tmpDir.newFolder().getAbsolutePath());
        targetDir = new Path(tmpDir.newFolder().getAbsolutePath());
    }

    @Test
    public void testSyncSingleRegion() throws IOException, InterruptedException {
        String r = "00141c192012ced16d54835996db80e9";
        String f = randomHFileOrRegionName();
        byte[] regionInfoBytes = readResourceAsBytes(HFileSyncer.class, "regioninfo");
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r + "/.regioninfo"), regionInfoBytes);
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r + "/o/" + f), "hfile");

        HFileSyncer syncer = new HFileSyncer(
                conf,
                new JobNamer("test_job"),
                hbaseRootDir,
                targetDir,
                ImmutableList.of(new TableRowRanges("table1", ImmutableList.of(RowRange.ALL))));
        syncer.run();

        assertFileContent(fs, new Path(targetDir + "/table1/" + r + "/.regioninfo"), regionInfoBytes);
        assertFileContent(fs, new Path(targetDir + "/table1/" + r + "/o/" + f), "hfile");
    }

    @Test
    public void testSyncMultipleRegions() throws IOException, InterruptedException {
        String r1 = "00141c192012ced16d54835996db80e9";
        String r2 = "00193bd96a455a61d48387dc15acbf0c";
        String f1 = randomHFileOrRegionName();
        String f2 = randomHFileOrRegionName();
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r1 + "/.regioninfo"),
                readResourceAsBytes(HFileSyncer.class, "regioninfo"));
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r2 + "/.regioninfo"),
                readResourceAsBytes(HFileSyncer.class, "regioninfo2"));
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r1 + "/o/" + f1), "f1");
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r2 + "/o/" + f2), "f2");

        HFileSyncer syncer = new HFileSyncer(
                conf,
                new JobNamer("test_job"),
                hbaseRootDir,
                targetDir,
                ImmutableList.of(new TableRowRanges("table1", ImmutableList.of(RowRange.ALL))));
        syncer.run();

        assertFileContent(fs, new Path(targetDir + "/table1/" + r1 + "/o/" + f1), "f1");
        assertFileContent(fs, new Path(targetDir + "/table1/" + r2 + "/o/" + f2), "f2");
    }

    @Test
    public void testRetryOnSyncFailure() throws IOException, InterruptedException {
        String r1 = "00141c192012ced16d54835996db80e9";
        String r2 = "00193bd96a455a61d48387dc15acbf0c";
        String f1 = randomHFileOrRegionName();
        String f2 = randomHFileOrRegionName();
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r1 + "/.regioninfo"),
                readResourceAsBytes(HFileSyncer.class, "regioninfo"));
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r2 + "/.regioninfo"),
                readResourceAsBytes(HFileSyncer.class, "regioninfo2"));
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r1 + "/o/" + f1), "f1");
        createFile(fs, new Path(hbaseRootDir + "/table1/" + r2 + "/o/" + f2), "f2");

        HFileSyncer syncer = spy(new HFileSyncer(
                conf,
                new JobNamer("test_job"),
                hbaseRootDir,
                targetDir,
                ImmutableList.of(new TableRowRanges("table1", ImmutableList.of(RowRange.ALL)))));

        // 模拟第一次同步 r1 失败
        doReturn(ImmutableList.of(r1))
                .when(syncer).runCopyRegionJob(eq(0), any(Path.class), any(Path.class), anyListOf(String.class));

        // 模拟第二次同步成功
        doReturn(Collections.emptyList())
                .when(syncer).runCopyRegionJob(eq(1), any(Path.class), any(Path.class), anyListOf(String.class));

        syncer.run();

        verify(syncer).runCopyRegionJob(eq(0), any(Path.class), any(Path.class),
                argThat(TestingUtils.<String>hasSizeOf(2)));
        verify(syncer).runCopyRegionJob(eq(1), any(Path.class), any(Path.class),
                argThat(TestingUtils.<String>hasSizeOf(1)));
        verify(syncer, never()).runCopyRegionJob(gt(1), any(Path.class), any(Path.class), anyListOf(String.class));
    }
}
