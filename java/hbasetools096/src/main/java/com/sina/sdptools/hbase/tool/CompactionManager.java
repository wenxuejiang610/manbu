package com.sina.sdptools.hbase.tool;

// Import Java libraries
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

// Import hadoop libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Import third libraries
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sina.sdptools.hbase.HBaseCluster;
import com.sina.sdptools.hbase.RegionServer;
import com.sina.sdptools.util.DateUtil;
import com.sina.sdptools.util.HaActiveNamenode;

// Compaction class
public class CompactionManager {
  private Logger LOG = LoggerFactory.getLogger(getClass().getSimpleName());
  private HBaseCluster hcs;
  private List<RegionServer> regionServers;
  private FileSystem fs;
  private String hbaseDir;
  private String baseNode;
  private int minFileCount;
  private int maxRegionCount;
  private int number = 0;
  private int notLocalNum = 0;
  private int regionCompacted = 0;
  private boolean checkLocality = false;

  // Handle arguments
  public CompactionManager(int minFileCount, int maxRegionCount, String zkPath, String baseNode, String hdfsPath,
      String hbaseDir, boolean checkLocality) throws IOException {
    Configuration config = new Configuration();
    config.set("fs.defaultFS", hdfsPath);
    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    this.fs = DistributedFileSystem.get(config);
    this.hcs = new HBaseCluster(zkPath, baseNode);
    this.minFileCount = minFileCount;
    this.maxRegionCount = maxRegionCount;
    this.hbaseDir = hbaseDir;
    this.baseNode = baseNode;
    this.checkLocality = checkLocality;
  }

  // Rolling Compaction
  public void rollingCompaction(String hostName) throws IOException, InterruptedException {
    if (this.regionServers == null) {
      this.regionServers = this.hcs.getClusterRegionServers();
    }
    RegionServer rs = this.hcs.getRegionServerByIP(hostName);
    Map<byte[], RegionLoad> serverRegions = rs.getRegionLoads();
    Preconditions.checkNotNull(serverRegions);
    List<Map.Entry<byte[], RegionLoad>> serverRegionsSortedByBlockLocality = sortRegionLoad(serverRegions);
    for (Map.Entry<byte[], RegionLoad> entry : serverRegionsSortedByBlockLocality) {
      if (checkTotalCompactCount()) {
        this.LOG.info("["+this.baseNode+"] [" +rs.getServername().getServerName()+ "] total compact count is reach,so break,number=" + this.number);
        break;
      }
      chooseRegionToCompact(rs, entry);
    }
    this.LOG
        .info(String
            .format(
                "[%s] regionserver [%s],totalRegionCount=%s,maxRegionNum=%s,compactedRegionNum=%s,notLocalRegionNum=%s",
                new Object[] {this.baseNode, rs.getServername().getServerName(), rs.getRegionLoads().size(),
                    this.maxRegionCount, this.regionCompacted, this.notLocalNum}));
  }

  // Choose region to compact
  public void chooseRegionToCompact(RegionServer rs, Map.Entry<byte[], RegionLoad> entry) throws IOException,
      InterruptedException {
    if (((RegionLoad) entry.getValue()).getStorefiles() >= this.minFileCount && checkNotSystemTable(HRegionInfo.getTable(entry.getKey()).getNameAsString())) {
      if (this.checkLocality) {
        if (!hfileBlockLocalityCheck(getRegionPath((byte[]) entry.getKey()), rs.getServername().getHostname())) {
          compact((byte[]) entry.getKey());
          this.regionCompacted += 1;
          this.number += 1;
        }
        else {
          this.notLocalNum += 1;
        }
      }
      else {
        compact((byte[]) entry.getKey());
        this.regionCompacted += 1;
        this.number += 1;
      }
    }
  }

  // Sort regions according StoreFile numbers
  public List<Map.Entry<byte[], RegionLoad>> sortRegionLoad(Map<byte[], RegionLoad> serverRegions) {
    List<Map.Entry<byte[], RegionLoad>> regions = Lists.newArrayList(serverRegions.entrySet());
    Collections.sort(regions, new Comparator<Map.Entry<byte[], RegionLoad>>() {
      public int compare(Map.Entry<byte[], RegionLoad> region1, Map.Entry<byte[], RegionLoad> region2) {
        int regionStorefilesNum1 = ((RegionLoad) region1.getValue()).getStorefiles();
        int regionStorefilesNum2 = ((RegionLoad) region2.getValue()).getStorefiles();
        if (regionStorefilesNum1 == regionStorefilesNum2
            && checkNotSystemTable(HRegionInfo.getTable(region1.getKey()).getNameAsString())
            && checkNotSystemTable(HRegionInfo.getTable(region2.getKey()).getNameAsString())) {
          long modifyTime1 = getRegionFileLastModifyTime(getRegionPath(region1.getKey()));
          long modifyTime2 = getRegionFileLastModifyTime(getRegionPath(region2.getKey()));
          return Long.valueOf(modifyTime1).compareTo(Long.valueOf(modifyTime2));
        }
        else {
          return -Integer.valueOf(regionStorefilesNum1).compareTo(Integer.valueOf(regionStorefilesNum2));
        }
      }
    });
    return regions;
  }

  public Path getRegionPath(byte[] regionName) {
    String encodeRegion = HRegionInfo.encodeRegionName(regionName);
    String tableName = HRegionInfo.getTable(regionName).getNameAsString();
    String regionPath = Joiner.on("/").join(this.hbaseDir, tableName, new Object[] { encodeRegion });
    return new Path(regionPath);
  }

  public long getRegionFileLastModifyTime(Path regionPath) {
    long modifyTime = Long.MAX_VALUE;
    try {
      FileStatus[] fileStatuses = this.fs.listStatus(regionPath);
      for (FileStatus cfState : fileStatuses) {
        if (cfState.isDirectory()) {
          FileStatus[] hfileArray = this.fs.listStatus(cfState.getPath());
          for (FileStatus hfile : hfileArray) {
            if (hfile.getModificationTime() < modifyTime) {
              modifyTime = hfile.getModificationTime();
            }
          }
        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return modifyTime;
  }

  // Check the ceiling of compaction count
  private boolean checkTotalCompactCount() {
    if (this.number >= this.maxRegionCount) {
      return true;
    }
    return false;
  }

  // Executing compaction on RegionServer
  public void rollingCompaction() throws IOException, InterruptedException {
    if (this.regionServers == null) {
      this.regionServers = this.hcs.getClusterRegionServers();
    }
    Preconditions.checkNotNull(this.regionServers);
    for (RegionServer rs : this.regionServers) {
      this.regionCompacted = 0;
      rollingCompaction(rs.getServername().getHostname());
      this.number = 0;// init the numbers of compaction add on 2014-09-08
    }
  }

  // Check whether hregion.hfile in the local RegionServer
  public boolean hfileBlockLocalityCheck(Path regionPath, String hostName) {
    boolean locality = true;
    try {
      FileStatus[] fileStatuses = this.fs.listStatus(regionPath);
      for (FileStatus cfState : fileStatuses) {
        if (cfState.isDirectory()) {
          FileStatus[] hfileArray = this.fs.listStatus(cfState.getPath());
          for (FileStatus hfile : hfileArray) {
            BlockLocation[] fileBlockLocations = this.fs.getFileBlockLocations(hfile, 0L, hfile.getLen());
            if (!blockLocalityHostsCheck(fileBlockLocations, hostName)) {
              locality = false;
            }
          }
        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return locality;
  }

  // Check locality of hregion.hfile
  public boolean blockLocalityHostsCheck(BlockLocation[] fileBlockLocations, String hostName) throws IOException {
    boolean flag = true;
    for (BlockLocation bl : fileBlockLocations) {
      boolean block = false;
      for (String host : bl.getHosts()) {
        if (Bytes.compareTo(Bytes.toBytes(host), Bytes.toBytes(hostName)) == 0) {
          block = true;
          break;
        }
      }
      if (!block) {
        flag = false;
      }
    }
    return flag;
  }

  // Exclude system tables while compacting
  public boolean checkNotSystemTable(String regionName) {
    if ((regionName.contains("-ROOT-")) || (regionName.contains(".META.")) || (regionName.contains("hbase:meta"))
        || (regionName.contains("hbase:namespace"))) {
      return false;
    }
    return true;
  }

  // Executing compaction
  public void compact(byte[] regionName) throws IOException, InterruptedException {
    HBaseAdmin admin = this.hcs.getAdmin();
    long startTime = System.currentTimeMillis();
    admin.majorCompact(regionName);
    long usedTime = System.currentTimeMillis()-startTime;
    this.LOG.info("["+this.baseNode+"]"+" region [" + HRegionInfo.encodeRegionName(regionName) + "]" + " from table ["
        + HRegionInfo.getTable(regionName).getNameAsString()
        + "] major compaction is complete ,used "+usedTime+"ms,it's  lastModifyTime is "
        + DateUtil.millisecond2date(getRegionFileLastModifyTime(getRegionPath(regionName))));
  }

  // Main function
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    args = new String[7];
    args[0] = "2";
    args[1] = "1";
    args[2] = "zk1.mars.grid.sina.com.cn:2181,zk2.mars.grid.sina.com.cn:2181,zk3.mars.grid.sina.com.cn:2181,zk4.mars.grid.sina.com.cn:2181,zk5.mars.grid.sina.com.cn:2181";
    args[3] = "/v09611-hbase";
    args[4] = "h002194";
    args[5] = "/hbase/data/default";
    args[6] = "false";
    if (args.length != 7) {
      System.out
          .println("Usage: minFileToCompact, maxCompactCount, zkQuorum, zkParentNode, hdfsPath, hbaseHDFSDir useLocatilyCheck");
      System.out
          .println("eg: 3 500 zk4.mars.grid.sina.com.cn:2181 /object-aggregate-hbase hdfs://nnobja.mars.grid.sina.com.cn:9000 /hbase true");
      return;
    }

    //BalanceRegion br = new BalanceRegion(args[2], args[3]);
    // br.balanceRegionBeforeCompact();

    String zkAddress = args[2];
    String nameserviceId = args[4];
    ActiveNodeInfo activeNodeInfo = HaActiveNamenode.getActiveNamenode(zkAddress, "hadoop-ha", nameserviceId);
    String hdfsPath = "hdfs://" + activeNodeInfo.getHostname() + ":" + activeNodeInfo.getPort();

    CompactionManager cm = new CompactionManager(Integer.parseInt(args[0]), Integer.parseInt(args[1]), zkAddress,
        args[3], hdfsPath, args[5], Boolean.parseBoolean(args[6]));
    try {
      cm.rollingCompaction();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
