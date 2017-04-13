package com.sina.sdptools.app.hfilesync.cli;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;
import com.sina.sdptools.app.hfilesync.core.HFileSyncer;
import com.sina.sdptools.app.hfilesync.core.RowRange;
import com.sina.sdptools.app.hfilesync.core.ShellUtils;
import com.sina.sdptools.app.hfilesync.core.TableRowRanges;
import com.sina.sdptools.app.hfilesync.util.JobNamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** 命令行入口 */
public class HFileSyncTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HFileSyncTool.class);

    public interface CommandLineOptions {
        @Option(longName = "hbase-root", description="hdfs://hostname:port/hbase")
        Path hbaseRootDir();

        @Option(longName = "target")
        Path targetDir();

        @Option(longName = "table", description = "table1,table2")
        String table();

        @Option(longName = "jobtracker", defaultToNull = true)
        String jobTracker();

        @Option(longName = "max_concurrent_copier", defaultValue = "5")
        String maxConcurrentCopier();

        @Option(longName = "skip_prefixes", defaultValue = "", description = "table1,row1;table2,row3")
        String skipPrefixes();

        @SuppressWarnings("unused")
        @Option(helpRequest = true)
        boolean help();
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLineOptions cli = CliFactory.parseArguments(CommandLineOptions.class, args);
        Configuration conf = new Configuration(getConf());

        List<String> tables = ImmutableList.copyOf(
                Splitter.on(',').split(cli.table()));
        LOG.info("HBase root directory (source): " + cli.hbaseRootDir());
        LOG.info("Target dir: " + cli.targetDir());
        LOG.info("Table(s): " + Joiner.on(',').join(tables));
        LOG.info("Max Concurrent Copier: " + cli.maxConcurrentCopier());
        LOG.info("Skip Prefixes: " + cli.skipPrefixes());

        conf.set("hfilesync.max_concurrent_copier", cli.maxConcurrentCopier());
        conf.set("hfilesync.skip_prefixes", cli.skipPrefixes());
        if (Strings.isNullOrEmpty(cli.jobTracker())) {
            LOG.warn("Option --jobtracker is not specified. DistCp maybe run locally.");
        } else {
            LOG.info("JobTracker: " + cli.jobTracker());
            conf.set("mapred.job.tracker", cli.jobTracker());
        }

        List<TableRowRanges> tableRanges = Lists.newArrayList();
        for (String table : tables) {
            tableRanges.add(new TableRowRanges(table, ImmutableList.of(RowRange.ALL)));
        }

        HFileSyncer sync = new HFileSyncer(
                conf,
                new JobNamer("SyncTable " + Joiner.on("/").join(tables)),
                cli.hbaseRootDir(),
                cli.targetDir(),
                tableRanges);
        sync.run();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ShellUtils.run(new HFileSyncTool(), args);
    }
}
