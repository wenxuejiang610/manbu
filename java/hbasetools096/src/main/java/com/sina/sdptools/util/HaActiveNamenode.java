package com.sina.sdptools.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;


public class HaActiveNamenode {
  public static CountDownLatch connectedSemaphore = new CountDownLatch(1);
  public static ActiveNodeInfo getActiveNamenode(String connectString, String hadoopHARoot, String nameserviceId)
      throws IOException, KeeperException, InterruptedException {
    int retryNum = 0;
    byte[] data = null;
    boolean retry = true;
    ZooKeeper zk = new ZooKeeper(connectString, 10000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
          connectedSemaphore.countDown();
        }
      }
    });
    connectedSemaphore.await();
    while (retry) {
      try {
        data = zk.getData("/" + hadoopHARoot + "/" + nameserviceId + "/ActiveStandbyElectorLock", false, null);
        retry = false;
      }
      catch (KeeperException ke) {
        if (++retryNum < 3) {
          continue;
        }
        throw ke;
      }
    }
    zk.close();
    ActiveNodeInfo result = null;
    if (data != null) {
      result = ActiveNodeInfo.parseFrom(data);
    }
    return result;
  }

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    String zkAddress = "zk1.mars.grid.sina.com.cn,zk2.mars.grid.sina.com.cn,zk3.mars.grid.sina.com.cn,zk4.mars.grid.sina.com.cn,zk5.mars.grid.sina.com.cn";
    ActiveNodeInfo activeNodeInfo = HaActiveNamenode.getActiveNamenode(zkAddress, "hadoop-ha", "gaia");
    System.out.println("Hostname:"+activeNodeInfo.getHostname());
    System.out.println("Port:"+activeNodeInfo.getPort());  }
}
