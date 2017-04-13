package com.sina.sdptools.app;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.core.RunTool;
import com.sina.sdptools.hbase.tool.BalanceRegion;
import com.sina.sdptools.hbase.tool.CompactionManager;

public class CompactionApp extends RunTool {
  private Logger log = LoggerFactory.getLogger(getClass().getSimpleName());

  @Override
  protected void printHelp(PrintStream out) {
    printCmdHelp(out, "", new String[] {},
        "Usage: minFileToCompact, maxCompactCount, zkQuorum, zkParentNode, hdfsPath, hbaseHDFSDir useLocatilyCheck");
    printCmdHelp(out, "", new String[] {},
        "eg: 3 500 zk4.mars.grid.sina.com.cn:2181 /object-aggregate-hbase hdfs://nnobja.mars.grid.sina.com.cn:9000 /hbase true");
  }

  @Override
  protected int getArgumentNumber() {
    return 7;
  }

  @Override
  public int exec(String[] args) throws Exception {
    log.info("This app for hbase0.94 compation");
    // BalanceRegion br = new BalanceRegion(args[2], args[3]);
    // br.balanceRegionBeforeCompact();
    final CompactionManager cm = new CompactionManager(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2],
        args[3], args[4], args[5], Boolean.parseBoolean(args[6]));
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
