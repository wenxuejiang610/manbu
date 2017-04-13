package com.sina.sdptools.app.hfilesync.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sina.sdptools.app.hfilesync.util.JobNamer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/** 主要的同步和重试逻辑，线程不安全。*/
public class HFileSyncer {

    private static final Logger LOG = LoggerFactory.getLogger(HFileSyncer.class);

    private static final int MAX_ROUNDS = 10;
    private static final int MAX_CONCURRENT_COPY_TASKS = 5;

    /** 每个分区的名字及开始结束的范围 */
    private static class RegionRange {
        private final String region;
        private final RowRange range;

        public RegionRange(String region, RowRange range) {
            this.region = MorePreconditions.checkNotNullOrEmpty(region);
            this.range = Preconditions.checkNotNull(range);
        }

        public String getRegion() {
            return region;
        }

        public RowRange getRange() {
            return range;
        }
    }

    private final Configuration conf;
    private final JobNamer jobNamer;
    private final Path hbaseRootDir;
    private final Path targetDir;
    private final List<TableRowRanges> tableRanges;

    private final Map<Path, RegionRange> regionRangeCache = Maps.newHashMap();

    public HFileSyncer(
            Configuration conf,
            JobNamer jobNamer,
            Path hbaseRootDir,
            Path targetDir,
            List<TableRowRanges> tableRanges) {
        this.conf = new Configuration(Preconditions.checkNotNull(conf));
        this.jobNamer = Preconditions.checkNotNull(jobNamer);
        this.hbaseRootDir = Preconditions.checkNotNull(hbaseRootDir);
        this.targetDir = Preconditions.checkNotNull(targetDir);
        this.tableRanges = ImmutableList.copyOf(Preconditions.checkNotNull(tableRanges));
    }

    public void run() throws IOException, InterruptedException {
        List<TableRowRanges> toSync = tableRanges;
        for (int round = 0; round < MAX_ROUNDS && !toSync.isEmpty(); round++) {
            ImmutableList.Builder<TableRowRanges> nextToSync = ImmutableList.builder();
            for (TableRowRanges tableRowRanges : toSync) {
                String table = tableRowRanges.getTable();
                List<RowRange> nextRowRanges = syncTable(round, table, tableRowRanges.getRanges());
                if (!nextRowRanges.isEmpty()) {
                    nextToSync.add(new TableRowRanges(table, nextRowRanges));
                }
            }
            toSync = nextToSync.build();
        }
    }

    private List<RowRange> getSkipRanges(String table) {
        String skipPrefixes = conf.get("hfilesync.skip_prefixes");
        List<String> prefixes = ImmutableList.of();
        if (!Strings.isNullOrEmpty(skipPrefixes)) {
            prefixes = ImmutableList.copyOf(Splitter.on(";").split(skipPrefixes));
        }
        List<RowRange> result = new ArrayList<RowRange>();
        for (String prefix : prefixes) {
            List<String> entry = ImmutableList.copyOf(Splitter.on(",").split(prefix));
            if (entry.size() == 2 && entry.get(0).equals(table)) {
                RowRange r = new RowRange(ByteString.copyFrom(entry.get(1).getBytes()));
                result.add(r);
            }
        }
        return result;
    }

    /** 同步单表数据，返回本次同步失败的范围，全部成功返回空列表 */
    private List<RowRange> syncTable(
            int round,
            String table,
            List<RowRange> ranges) throws IOException, InterruptedException {
        MorePreconditions.checkNotNullOrEmpty(table);
        Preconditions.checkNotNull(ranges);

        if (ranges.isEmpty()) {
            return Collections.emptyList();
        }

        FileSystem srcFs = getSourceFileSystem();
        FileSystem targetFs = getTargetFileSystem();
        Path sourcePath = new Path(hbaseRootDir, table);
        Path targetPath = new Path(targetDir, table);

        List<RowRange> skipRanges = getSkipRanges(table);

        for (RowRange range : skipRanges) {
            LOG.info("skipRange: " + range.getStartRow().toString() + "  " + range.getEndRow().toString());
        }

        Map<String, RegionRange> regionsToSync = Maps.newHashMap();
        for (Path path : listRegionInfoFiles(srcFs, sourcePath)) {
            RegionRange r;
            try {
                r = readRegionRange(srcFs, path);
            } catch (FileNotFoundException e) {
                // .regioninfo may be deleted because of region split. We will
                // ignore such case. This should be no problem, because we should
                // have already seen its children.
                LOG.warn("ignore " + path + " because it might be deleted due to region split");
                continue;
            }

            boolean can_skip = false;
            for (RowRange skipRange : skipRanges) {
                if (RowRange.contain(skipRange, r.getRange()))  {
                    can_skip = true;
                    LOG.info("Skip Region " + table + "," + r.getRegion());
                    break;
                }
            }

            if (!can_skip && RowRange.intersectAny(r.getRange(), ranges)) {
                regionsToSync.put(r.getRegion(), r);
            }
        }
        if (regionsToSync.isEmpty()) {
            throw new AssertionError("No region contains ranges: " + ranges + "?");
        }

        LOG.info("Need to sync " + regionsToSync.size() + " region(s) of " + table + " at round " + round);
        for(Map.Entry<String, RegionRange> region : regionsToSync.entrySet()) {
            LOG.info("prepare to sync " + table + "," + region.getKey());
        }

        targetFs.mkdirs(targetPath);
        List<String> failedRegions = runCopyRegionJob(
                round, sourcePath, targetPath, ImmutableList.copyOf(regionsToSync.keySet()));
        if (failedRegions.isEmpty()) {
            return Collections.emptyList();
        }
        LOG.warn("There are " + failedRegions.size() + " failed regions(s), which may be caused by compactions");

        Set<RowRange> failedRanges = Sets.newHashSet();
        for (String r : failedRegions) {
            RegionRange regionRange = regionsToSync.get(r);
            failedRanges.add(regionRange.getRange());
        }
        return ImmutableList.copyOf(failedRanges);
    }

    @VisibleForTesting
   public List<String> runCopyRegionJob(
            int round,
            Path sourcePath,
            Path targetPath,
            List<String> regions) throws IOException, InterruptedException {
        int max_concurrent_copier = Integer.parseInt(conf.get("hfilesync.max_concurrent_copier", ""+MAX_CONCURRENT_COPY_TASKS));
        if (max_concurrent_copier < 1 || max_concurrent_copier > 20000) {
            max_concurrent_copier = MAX_CONCURRENT_COPY_TASKS;
        }
        return CopyRegionJob.copy(
                conf,
                jobNamer,
                sourcePath,
                targetPath,
                regions,
                round,
                max_concurrent_copier);
    }

    private RegionRange readRegionRange(FileSystem fs, Path path) throws IOException {
        RegionRange r = regionRangeCache.get(path);
        if (r != null) {
            return r;
        }
        r = doReadRegionRangeNoCache(fs, path);
        regionRangeCache.put(path, r);
        return r;
    }

    private RegionRange doReadRegionRangeNoCache(FileSystem fs, Path path) throws IOException {
        HRegionInfo94 ri = new HRegionInfo94();
        FSDataInputStream in = fs.open(path);
        try {
            ri.readFields(in);
        } finally {
            MoreIOUtils.closeQuietly(in);
        }

        return new RegionRange(
                ri.getEncodedName(),
                new RowRange(ByteString.copyFrom(ri.getStartKey()), ByteString.copyFrom(ri.getEndKey())));
    }

    private static List<Path> listRegionInfoFiles(FileSystem fs, Path tableDir) throws IOException {
        FileStatus[] statuses = fs.globStatus(new Path(tableDir, "*/.regioninfo"));
        if (statuses == null) {
            throw new FileNotFoundException(tableDir.toString());
        }
        ImmutableList.Builder<Path> results = ImmutableList.builder();
        for (FileStatus status : statuses) {
            if (!status.isDir()) {
                results.add(status.getPath());
            }
        }
        return results.build();
    }

    private FileSystem getSourceFileSystem() throws IOException {
        return hbaseRootDir.getFileSystem(conf);
    }

    private FileSystem getTargetFileSystem() throws IOException {
        return targetDir.getFileSystem(conf);
    }
}
