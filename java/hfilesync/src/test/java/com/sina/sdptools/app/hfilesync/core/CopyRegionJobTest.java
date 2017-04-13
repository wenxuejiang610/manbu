package com.sina.sdptools.app.hfilesync.core;

import com.google.common.collect.ImmutableList;
import com.sina.sdptools.app.hfilesync.core.CopyRegionJob;
import com.sina.sdptools.app.hfilesync.util.JobNamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static com.sina.sdptools.app.hfilesync.core.TestingUtils.assertFileContent;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.assertFileNotExist;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.createFile;
import static com.sina.sdptools.app.hfilesync.core.TestingUtils.randomHFileOrRegionName;
import static org.junit.Assert.assertEquals;

public class CopyRegionJobTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private Configuration conf;
    private FileSystem fs;
    private Path sourceDir;
    private Path targetDir;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        fs = FileSystem.getLocal(conf);
        sourceDir = new Path(tmpDir.newFolder().getAbsolutePath());
        targetDir = new Path(tmpDir.newFolder().getAbsolutePath());
    }

    @Test
    public void testCopy() throws IOException, InterruptedException {
        String r1 = randomHFileOrRegionName();
        String r2 = randomHFileOrRegionName();
        String f1 = randomHFileOrRegionName();
        String f2 = randomHFileOrRegionName();
        String f3 = randomHFileOrRegionName();
        createFile(fs, new Path(sourceDir + "/" + r1 + "/" + f1), "f1");
        createFile(fs, new Path(sourceDir + "/" + r1 + "/" + f2), "f2");
        createFile(fs, new Path(sourceDir + "/" + r2 + "/" + f3), "f3");
        CopyRegionJob.copy(conf, new JobNamer("test_job"), sourceDir, targetDir, ImmutableList.of(r1, r2), 0, 1);
        assertFileContent(fs, new Path(targetDir + "/" + r1 + "/" + f1), "f1");
        assertFileContent(fs, new Path(targetDir + "/" + r1 + "/" + f2), "f2");
        assertFileContent(fs, new Path(targetDir + "/" + r2 + "/" + f3), "f3");
    }

    @Test
    public void testOnlyCopyHFiles() throws IOException, InterruptedException {
        String r = randomHFileOrRegionName();
        String f1 = randomHFileOrRegionName();
        String f2 = "should_be_ignored";
        createFile(fs, new Path(sourceDir + "/" + r + "/" + f1), "f1");
        createFile(fs, new Path(sourceDir + "/" + r + "/" + f2), "f2");
        CopyRegionJob.copy(conf, new JobNamer("test_job"), sourceDir, targetDir, ImmutableList.of(r), 0, 1);
        assertFileContent(fs, new Path(targetDir + "/" + r + "/" + f1), "f1");
        assertFileNotExist(fs, new Path(targetDir, "/" + r + "/" + f2));
    }

    @Test
    public void testCopyRecursively() throws IOException, InterruptedException {
        String r = randomHFileOrRegionName();
        String f1 = randomHFileOrRegionName();
        String f2 = randomHFileOrRegionName();
        createFile(fs, new Path(sourceDir + "/" + r + "/a/" + f1), "f1");
        createFile(fs, new Path(sourceDir + "/" + r + "/a/b/" + f2), "f2");
        CopyRegionJob.copy(conf, new JobNamer("test_job"), sourceDir, targetDir, ImmutableList.of(r), 0, 1);
        assertFileContent(fs, new Path(targetDir + "/" + r + "/a/" + f1), "f1");
        assertFileNotExist(fs, new Path(targetDir, "/" + r + "/a/b/" + f2));
    }

    @Test
    public void testReturnFailedRegion() throws IOException, InterruptedException {
        String r1 = randomHFileOrRegionName();
        String r2 = randomHFileOrRegionName(); // does not exist
        String f1 = randomHFileOrRegionName();
        createFile(fs, new Path(sourceDir + "/" + r1 + "/" + f1), "f1");
        List<String> failed = CopyRegionJob.copy(
                conf, new JobNamer("test_job"), sourceDir, targetDir, ImmutableList.of(r1, r2), 0, 1);
        assertFileContent(fs, new Path(targetDir + "/" + r1 + "/" + f1), "f1");
        assertEquals(ImmutableList.of(r2), failed);
    }
}
