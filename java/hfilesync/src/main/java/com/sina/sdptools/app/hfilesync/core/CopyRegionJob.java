package com.sina.sdptools.app.hfilesync.core;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.sina.sdptools.app.hfilesync.util.JobNamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

public final class CopyRegionJob {

    // 由于 slf4j 版本兼容问题，避免在 MapReduce 中使用 slf4j，直接使用 System.out 或 System.err 即可
    // private static final Logger LOG = LoggerFactory.getLogger(CopyRegionJob.class);

    private static final String SOURCE_DIR_CONFIG_KEY = "hfilesync.source.dir";
    private static final String TARGET_DIR_CONFIG_KEY = "hfilesync.target.dir";

    private static class DoCopyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    private static class DoCopyReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        private Path sourceDir;
        private Path targetDir;
        private FileSystem srcFs;
        private FileSystem targetFs;
        private Counter copiedBytesCounter;
        private Counter copiedFilesCounter;
        private Counter copiedRegionsCounter;
        private Counter failedRegionCounter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            sourceDir = getPathFromConfiguration(conf, SOURCE_DIR_CONFIG_KEY);
            targetDir = getPathFromConfiguration(conf, TARGET_DIR_CONFIG_KEY);

            Preconditions.checkState(sourceDir != null);
            Preconditions.checkState(targetDir != null);

            srcFs = sourceDir.getFileSystem(conf);
            targetFs = targetDir.getFileSystem(conf);

            copiedBytesCounter = context.getCounter("hfilesync", "copied.bytes");
            copiedFilesCounter = context.getCounter("hfilesync", "copied.files");
            copiedRegionsCounter = context.getCounter("hfilesync", "copied.regions");
            failedRegionCounter = context.getCounter("hfilesync", "failed.regions");
        }

        @Nullable
        private static Path getPathFromConfiguration(Configuration conf, String key) {
            String value = conf.get(key);
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            return new Path(value);
        }

        @Override
        protected void reduce(Text region, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Path regionDir = new Path(sourceDir, region.toString());
            try {
                copyFilesRecursively(regionDir, targetDir, context);
                copiedRegionsCounter.increment(1);
            } catch (IOException e) {
                System.err.println("Failed to copy " + regionDir);
                e.printStackTrace(System.err);
                context.write(region, NullWritable.get());
                failedRegionCounter.increment(1);
            }
        }

        private void copyFilesRecursively(
                Path sourcePath,
                Path targetPath,
                Context context) throws IOException {
            FileStatus fileStatus = srcFs.getFileStatus(sourcePath);
            if (fileStatus == null) {
                throw new IOException("No such file: " + sourcePath);
            }
            if (fileStatus.isDir()) {
                Path subpath = new Path(targetPath, sourcePath.getName());
                FileStatus[] fileStatuses = srcFs.listStatus(sourcePath);
                if (fileStatuses == null) {
                    throw new IOException("No such dir: " + sourcePath);
                }
                // 源文件是目录
                targetFs.mkdirs(subpath);
                for (FileStatus s : fileStatuses) {
                    copyFilesRecursively(s.getPath(), subpath, context);
                }
            } else {
                // 源文件是普通文件
                if (!sourcePath.getName().matches("[0-9a-f]{32}") &&
                        !Objects.equal(sourcePath.getName(), ".regioninfo")) {
                    // 只复制 HFile，忽略其它文件，不然后续 MR 读 HFile 时会报错
                    System.out.println("Ignore " + sourcePath);
                    return;
                }
                copyRegularFile(sourcePath, targetPath, context);
            }
        }

        private void copyRegularFile(Path sourcePath, Path targetDir, Context context) throws IOException {
            reportStatus(context, "Copying " + sourcePath);

            // 先将文件复制至临时目录，以免复制过程中源文件被删导致不完整
            Path tmpDir = new Path(targetDir, ".tmp");
            Path tmpTargetPath = new Path(tmpDir, sourcePath.getName());

            InputStream in = null;
            OutputStream out = null;
            try {
                in = srcFs.open(sourcePath);
                out = targetFs.create(tmpTargetPath, /* overwrite */ true);
                byte[] buffer = new byte[16*1024*1024]; // 16M
                int n;
                long lastCopyTimestamp = System.currentTimeMillis();
                while ((n = in.read(buffer)) > 0) {
                    out.write(buffer, 0, n);
                    copiedBytesCounter.increment(n);
                    long currentTimestamp = System.currentTimeMillis();
                    reportStatus(context, String.format("Copying %s (%.2f MB/s)",
                            sourcePath.toString(),
                            (1000.0 * n) / ((currentTimestamp - lastCopyTimestamp) * 1024 * 1024)));
                    lastCopyTimestamp = currentTimestamp;
                }
                out.flush();
                copiedFilesCounter.increment(1);
            } finally {
                MoreIOUtils.closeQuietly(in);
                MoreIOUtils.closeQuietly(out);
            }

            Path finalTargetPath = new Path(targetDir, sourcePath.getName());
            targetFs.rename(tmpTargetPath, finalTargetPath);
            targetFs.delete(tmpDir, true);
        }

        private void reportStatus(Context context, String status) {
            System.err.println(status);
            context.setStatus(status);
        }
    }

    private CopyRegionJob() {}

    /** 复制 HFile，返回失败的分区供下次尝试 */
    public static List<String> copy(
            Configuration conf,
            JobNamer jobNamer,
            Path sourceDir,
            Path targetDir,
            List<String> regions,
            int round,
            int maxNumOfTasks) throws IOException, InterruptedException {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(sourceDir);
        Preconditions.checkNotNull(targetDir);
        Preconditions.checkNotNull(regions);

        Configuration newConf = new Configuration(conf);
        newConf.set(SOURCE_DIR_CONFIG_KEY, sourceDir.toString());
        newConf.set(TARGET_DIR_CONFIG_KEY, targetDir.toString());

        // 禁用「试探性执行」，避免传输额外数据
        newConf.setBoolean("mapred.map.tasks.speculative.execution", false);
        newConf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        // 禁止输出压缩
        newConf.setBoolean("mapred.output.compress", false);

        Path tmpDir = createTmpDir(newConf, targetDir.getFileSystem(newConf));
        try {
            Path inPath = new Path(tmpDir, "in");
            Path outPath = new Path(tmpDir, "out");

            // 准备输入
            writeStringsToFile(newConf, inPath, regions);

            Job job = new Job(newConf);
            job.setJobName(jobNamer.nextJobName());
            job.setJarByClass(CopyRegionJob.class);
            job.setMapperClass(DoCopyMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, inPath);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(DoCopyReducer.class);
            job.setNumReduceTasks(guessNumOfReducers(regions.size(), maxNumOfTasks));
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outPath);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.submit();
            job.waitForCompletion(true);
            if (!job.isSuccessful()) {
                throw new IOException("Job failed: " + job.getJobID() + "(" + job.getTrackingURL() + ")");
            }

            return readStringsFromDir(newConf, outPath);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } finally {
            deleteDirQuietly(newConf, tmpDir);
        }
    }

    private static Path createTmpDir(Configuration conf, FileSystem fs) throws IOException {
        Path tmpDir = new Path(conf.get("tmp.dir", "/tmp"));
        Path path = new Path(tmpDir, "hfilesync_" + System.currentTimeMillis());
        fs.mkdirs(path);
        return path.makeQualified(fs);
    }

    private static void writeStringsToFile(Configuration conf, Path path, List<String> strings) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        Writer writer = new OutputStreamWriter(fs.create(path), Charsets.UTF_8);
        try {
            for (String s : strings) {
                writer.write(s);
                writer.write('\n');
            }
            writer.flush();
        } finally {
            MoreIOUtils.closeQuietly(writer);
        }
    }

    private static List<String> readStringsFromDir(Configuration conf, Path path) throws IOException {
        ImmutableList.Builder<String> results = ImmutableList.builder();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(path);
        if (statuses == null) {
            throw new FileNotFoundException(path.toString());
        }
        for (FileStatus status : statuses) {
            if (!status.isDir()) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(status.getPath()), Charsets.UTF_8));
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        results.add(line);
                    }
                } finally {
                    MoreIOUtils.closeQuietly(reader);
                }
            }
        }
        return results.build();
    }

    private static void deleteDirQuietly(Configuration conf, Path tmpDir) {
        try {
            FileSystem fs = tmpDir.getFileSystem(conf);
            fs.delete(tmpDir, true);
        } catch (IOException e) {
            // ignored
        }
    }

    private static int guessNumOfReducers(int numOfRegions, int maxNumOfTasks) {
        // 默认每个分区 1G，这里计划每个任务复制 4G 数据，但最多不能超过 maxNumOfTasks
        int n = numOfRegions / 4;
        if (n == 0) {
            return 1;
        } else if (n > maxNumOfTasks) {
            return maxNumOfTasks;
        } else {
            return n;
        }
    }
}
