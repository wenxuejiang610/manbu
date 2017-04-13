package com.sina.sdptools.app;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.core.RunTool;
import com.sina.sdptools.hbase.VerifyTabledataMR;


public class VerifyTabledataApp extends RunTool {
  private Logger log = LoggerFactory.getLogger(getClass().getSimpleName());

  @Override
  protected void printHelp(PrintStream out) {
    printCmdHelp(out, "", new String[] {},
        "Usage: starttime, endtime, zk1, zk2, zkParentNode1, zkParentNode2, tableName1,tableName2,rm,jarpath");
    printCmdHelp(
        out,
        "",
        new String[] {},
        "eg: 1426950550000 1426950550000 zk4.eos.grid.sina.com.cn,zk1.eos.grid.sina.com.cn zk4.eos.grid.sina.com.cn,zk1.eos.grid.sina.com.cn  /commoncomment-hbase /commoncomment-hbase msgbox msgbox h021242.eos.grid.sina.com.cn /Users/dapple/Documents/work/sina/toolbox/java/hbasetools096/target/hbasetools096-1.0.0-jar-with-dependencies.jar");
  }

  @Override
  protected int getArgumentNumber() {
    return 10;
  }

  @Override
  public int exec(String[] args) throws Exception {
    log.info("This app for Verify hbase0.96 table data");

    VerifyTabledataMR mr = new VerifyTabledataMR();

    try {
      mr.execute(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7],args[8],args[9]);
    }
    catch (final Exception e) {
      log.error(e.getMessage());
      return 1;
    }
    return 0;
  }
}
