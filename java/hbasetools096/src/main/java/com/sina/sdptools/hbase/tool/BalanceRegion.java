package com.sina.sdptools.hbase.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class BalanceRegion {
  private static Logger LOG = Logger.getLogger(BalanceRegion.class);
  private final HBaseAdmin admin;

  public HBaseAdmin getAdmin() {
    return this.admin;
  }


  public BalanceRegion(final String zookeeper, final String hbaseDir) throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zookeeper);
    conf.set("zookeeper.znode.parent", hbaseDir);
    this.admin = new HBaseAdmin(conf);
  }

  public void balanceRegionBeforeCompact() {
    final HBaseAdmin admin = getAdmin();
    try {
      admin.setBalancerRunning(true, true);
      LOG.info("balance trigered");

      Thread.sleep(60000L);
      admin.setBalancerRunning(false, false);
      LOG.info("balance complete");
    }
    catch (final IOException e) {
      e.printStackTrace();
    }
    catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }
}