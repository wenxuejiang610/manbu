package com.sina.sdptools.app;

import java.io.PrintStream;

import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.hbase.tool.CompactionManager;
import com.sina.sdptools.util.HaActiveNamenode;
import com.sina.sdptools.core.RunTool;

public class CompactionApp extends RunTool {
  private Logger log = LoggerFactory.getLogger(getClass().getSimpleName());
  @Override
  protected void printHelp(PrintStream out) {
    printCmdHelp(out, "", new String[] {},
        "Usage: minFileToCompact, maxCompactCount, zkQuorum, zkParentNode, hdfsNameService, hbaseHDFSDir useLocatilyCheck");
    printCmdHelp(out, "", new String[] {},
        "eg: 3 500 zk4.mars.grid.sina.com.cn:2181 /v09611-hbase h002194 /hbase/data/default true");
  }

  @Override
  protected int getArgumentNumber() {
    return 7;
  }

  @Override
  public int exec(String[] args) throws Exception {
    log.info("This app for hbase0.96 compation");
    //BalanceRegion br = new BalanceRegion(args[2], args[3]);
    //br.balanceRegionBeforeCompact();
    
    String zkAddress = args[2];
    String nameserviceId = args[4];
    ActiveNodeInfo activeNodeInfo = HaActiveNamenode.getActiveNamenode(zkAddress, "hadoop-ha", nameserviceId);
    String hdfsPath = "hdfs://"+activeNodeInfo.getHostname()+":"+activeNodeInfo.getPort();
   
    final CompactionManager cm = new CompactionManager(Integer.parseInt(args[0]), Integer.parseInt(args[1]), zkAddress,
        args[3], hdfsPath, args[5], Boolean.parseBoolean(args[6]));
    try {
      cm.rollingCompaction();
    }
    catch (final InterruptedException e) {
      log.error(e.getMessage());
      return 1;
    }
    return 0;
  }

}
