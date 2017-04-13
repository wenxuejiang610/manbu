package com.sina.sdptools.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HConnectable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class VerifyTabledataMR {
  public static final String zkAddress_yf = "zk4.eos.grid.sina.com.cn,zk3.eos.grid.sina.com.cn,zk2.eos.grid.sina.com.cn,zk1.eos.grid.sina.com.cn,zk5.eos.grid.sina.com.cn";
  public static final String zkAddress_bx = "zk4.mars.grid.sina.com.cn,zk3.mars.grid.sina.com.cn,zk2.mars.grid.sina.com.cn,zk1.mars.grid.sina.com.cn,zk5.mars.grid.sina.com.cn";
  private static final Log LOG = LogFactory.getLog(VerifyTabledataMR.class);

  public final static String NAME = "verifyrep";
  static long startTime = 0;
  static long endTime = 0;
  static String tableName1 = null;
  static String tableName2 = null;
  static String families = null;
  static String zk1 = null;
  static String zk2 = null;
  static String znode1 = null;
  static String znode2 = null;

  public static class Verifier extends TableMapper<ImmutableBytesWritable, Put> {

    public static enum Counters {
      GOODROWS, BADROWS
    }

    public static Configuration conf1 = HBaseConfiguration.create();

    private ResultScanner replicatedScanner;

    @Override
    public void map(ImmutableBytesWritable row, final Result value, Context context) throws IOException {
      if (replicatedScanner == null) {
        Configuration conf = context.getConfiguration();
        final Scan scan = new Scan();
        scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
        long startTime = conf.getLong(NAME + ".startTime", 0);
        long endTime = conf.getLong(NAME + ".endTime", 0);
        String families = conf.get(NAME + ".families", null);
        String znode2 = conf.get(NAME + ".znode2", null);
        String zk2 = conf.get(NAME + ".zk2", null);
        final String table_name = conf.get(NAME + ".tableName");

        conf1.set("hbase.zookeeper.quorum", zk2);
        conf1.set("hbase.zookeeper.property.clientPort", "2181");
        conf1.set("zookeeper.znode.parent", znode2);
        if (families != null) {
          String[] fams = families.split(",");
          for (String fam : fams) {
            scan.addFamily(Bytes.toBytes(fam));
          }
        }
        if (startTime != 0) {
          scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
        }
        HConnectionManager.execute(new HConnectable<Void>(conf1) {
          @Override
          public Void connect(HConnection conn) throws IOException {
            HTable replicatedTable = new HTable(conf1, table_name);
            scan.setStartRow(value.getRow());
            replicatedScanner = replicatedTable.getScanner(scan);
            return null;
          }
        });
      }
      Result res = replicatedScanner.next();
      try {
        Result.compareResults(value, res);
        context.getCounter(Counters.GOODROWS).increment(1);
      }
      catch (Exception e) {
        LOG.warn("Bad row", e);
        context.getCounter(Counters.BADROWS).increment(1);
      }
    }

    protected void cleanup(Context context) {
      if (replicatedScanner != null) {
        replicatedScanner.close();
        replicatedScanner = null;
      }
    }
  }

  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    conf.set(NAME + ".tableName", tableName2);
    conf.setLong(NAME + ".startTime", startTime);
    conf.setLong(NAME + ".endTime", endTime);
    conf.set(NAME + ".zk1", zk1);
    conf.set(NAME + ".zk2", zk2);
    conf.set(NAME + ".znode2", znode2);
    if (families != null) {
      conf.set(NAME + ".families", families);
    }
    Job job = new Job(conf, NAME + "_" + tableName1);
    job.setJarByClass(VerifyTabledataMR.class);
    Scan scan = new Scan();
    if (startTime != 0) {
      scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if (families != null) {
      String[] fams = families.split(",");
      for (String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }
    TableMapReduceUtil.initTableMapperJob(tableName1, scan, Verifier.class, null, null, job);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);

    return job;
  }

  private static boolean doCommandLine(final String[] args) {
    if (args.length < 2) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }

        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }

        final String zk1ArgKey = "--zk1=";
        if (cmd.startsWith(zk1ArgKey)) {
          zk1 = cmd.substring(zk1ArgKey.length());
          continue;
        }
        final String zk2ArgKey = "--zk2=";
        if (cmd.startsWith(zk2ArgKey)) {
          zk2 = cmd.substring(zk2ArgKey.length());
          continue;
        }
        final String znode1ArgKey = "--znode1=";
        if (cmd.startsWith(znode1ArgKey)) {
          znode1 = cmd.substring(znode1ArgKey.length());
          continue;
        }
        final String znode2ArgKey = "--znode2=";
        if (cmd.startsWith(znode2ArgKey)) {
          znode2 = cmd.substring(znode2ArgKey.length());
          continue;
        }
        final String tableName1ArgKey = "--tableName1=";
        if (cmd.startsWith(tableName1ArgKey)) {
          tableName1 = cmd.substring(tableName1ArgKey.length());
          continue;
        }
        final String tableName2ArgKey = "--tableName2=";
        if (cmd.startsWith(tableName2ArgKey)) {
          tableName2 = cmd.substring(tableName2ArgKey.length());
          continue;
        }

      }
    }
    catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: verifyrep [--starttime=X]" + " [--stoptime=Y] [--families=A] <peerid> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" endtime     end of the time range");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" tablename    Name of the table to verify");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To verify the data of table1,table2");
    System.err.println(" $ java " + "com.sina.sdptools.hbase.VerifyTabledataMR"
        + " --starttime=1426950550000 --endtime=1426950550000"
        + " --zk1=zk4.eos.grid.sina.com.cn,zk1.eos.grid.sina.com.cn"
        + " --zk2=zk4.eos.grid.sina.com.cn,zk1.eos.grid.sina.com.cn" + " --znode1=/commoncomment-hbase"
        + " --znode2=/commoncomment-hbase" + " --tableName1=msgbox" + " --tableName2=msgbox");
  }

  /**
   * Main entry point.
   *
   * @param args
   *          The command line parameters.
   * @throws Exception
   *           When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    args = new String[8];
    args[0] = "--starttime=1426950550000";
    args[1] = "--endtime=1426950600000";

    args[2] = "--zk1=" + zkAddress_bx;
    args[3] = "--zk2=" + zkAddress_bx;
    args[4] = "--znode1=/v09611-hbase";
    args[5] = "--znode2=/v09611-hbase";
    args[6] = "--tableName1=msgbox-deyou";
    args[7] = "--tableName2=msgbox-deyou";
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", args[2].split("=")[1]);
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", args[4].split("=")[1]);
    conf.set("fs.defaultFS", "hdfs://h002194.mars.grid.sina.com.cn:9000");
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "h002194.mars.grid.sina.com.cn:8032");
    conf.set("yarn.resourcemanager.scheduler.address", "h002194.mars.grid.sina.com.cn:8030");
    conf.set("mapred.jar",
        "/Users/dapple/Documents/work/sina/toolbox/java/hbasetools096/target/hbasetools096-1.0.0.jar");
    for(String str:args){
      System.out.println("args="+str);
    }
    
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }

  public void execute(String starttime, String endtime, String zk1, String zk2, String znode1, String znode2,
      String table1, String table2, String rm, String jarpath) throws IOException, ClassNotFoundException,
      InterruptedException {
    String[] args = new String[10];
    args[0] = "--starttime=" + starttime;
    args[1] = "--endtime=" + endtime;

    args[2] = "--zk1=" + zk1;
    args[3] = "--zk2=" + zk2;
    args[4] = "--znode1=" + znode1;
    args[5] = "--znode2=" + znode2;
    args[6] = "--tableName1=" + table1;
    args[7] = "--tableName2=" + table2;
    args[8] = "--rm=" + rm;
    args[9] = "--jarpath=" + jarpath;
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", args[2].split("=")[1]);
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("zookeeper.znode.parent", args[4].split("=")[1]);
    conf.set("fs.defaultFS", "hdfs://" + rm + ":9000");
    conf.set("yarn.resourcemanager.address", rm + ":8032");
    conf.set("yarn.resourcemanager.scheduler.address", rm + ":8030");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("mapred.jar", jarpath);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    //    conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
    //        +"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
    //        +"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
    //        +"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
    //        +"$YARN_HOME/*,$YARN_HOME/lib/*,"
    //        +"$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*");

    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
