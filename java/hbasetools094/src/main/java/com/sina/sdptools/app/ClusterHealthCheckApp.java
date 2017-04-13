package com.sina.sdptools.app;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.core.RunTool;
public class ClusterHealthCheckApp extends RunTool {
	
  private Logger log = LoggerFactory.getLogger(getClass().getSimpleName());
  
  private HBaseAdmin hbaseAdmin = null;
  private Configuration conf = null;

  private HTable hTable = null;
  
  @Override
  protected void printHelp(PrintStream out) {
	  	out.println(toolId + " <hbase.zookeeper.quorum> <zookeeper.znode.parent>");
		out.println();
    }

  @Override
  protected int getArgumentNumber() {
    return 2;
  }

  public static void main(String[] args) {
	  String strs [] = {"zk1.mars.grid.sina.com.cn:2181,zk2.mars.grid.sina.com.cn:2181,zk3.mars.grid.sina.com.cn:2181,zk4.mars.grid.sina.com.cn:2181,zk5.mars.grid.sina.com.cn:2181","/v09410-hbase"};
	  ClusterHealthCheckApp healthTestApp = new ClusterHealthCheckApp();
	  healthTestApp.exec(strs);
  }
 
  
  @Override
	public  int exec(String[] args) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", args[0]);
		conf.set("zookeeper.znode.parent", args[1]);
		
		byte tableName[] = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
		byte cf[] = Bytes.toBytes("cf1"); 
	
		try {
			hbaseAdmin = new HBaseAdmin(conf);
			if (null != hbaseAdmin && createTable(tableName, cf) == 0) {
				if (putAndGet(tableName, cf) == 0 & deleteTable(tableName) == 0) {
					log.info("*******  cluster is healthy *******");
				}
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} finally {
			closeResource();
		}
		return 0;
	}
  
	private int createTable(byte[] tableName,byte[] cf) {
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
		hTableDescriptor.addFamily(hColumnDescriptor);
		
		try {
			long start = System.currentTimeMillis();
			hbaseAdmin.createTable(hTableDescriptor);
			log.info("======  create table("+Bytes.toString(tableName)+") success in "+(System.currentTimeMillis()-start)+"ms ======");
		} catch (IOException e) {
		    log.info("====== filed create table ======");
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private int putAndGet(byte[] tableName,byte[] cf) {
		try {
			hTable = new HTable(conf, tableName);
			hTable.setAutoFlush(true);
			
			byte rowkey[] = Bytes.toBytes("r1");
			
			long start = System.currentTimeMillis();
			Put put = new Put(rowkey);
			put.add(cf, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
			hTable.put(put);
			log.info("======  put value success in "+(System.currentTimeMillis()-start)+"ms ======");
			
			start = System.currentTimeMillis();
			Delete delete = new Delete(rowkey);
			hTable.delete(delete);
			log.info("======  delete value success in "+(System.currentTimeMillis()-start)+"ms ======");

		} catch (IOException e) {
			log.info("======  put or delete value failed ======");
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private int deleteTable(byte[] tableName) {
		try {
			long start = System.currentTimeMillis();
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			log.info("======  delete table("+Bytes.toString(tableName)+") success in "+(System.currentTimeMillis()-start)+"ms ======");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
  
	private int closeResource() {
		try {
			if (null != hbaseAdmin)
				hbaseAdmin.close();
			if (null != hTable)
				hTable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return 0;
	}

}
