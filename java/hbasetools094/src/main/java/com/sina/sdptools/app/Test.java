package com.sina.sdptools.app;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.common.primitives.Bytes;
import com.sina.sdptools.hbase.HBaseCluster;
import com.sina.sdptools.hbase.tool.CompactionManager;
import com.sun.org.apache.bcel.internal.generic.NEW;

public class Test {

	static Configuration conf;

	static{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "zk1.mars.grid.sina.com.cn:2181,zk2.mars.grid.sina.com.cn:2181,zk3.mars.grid.sina.com.cn:2181,zk4.mars.grid.sina.com.cn:2181,zk5.mars.grid.sina.com.cn:2181");
		conf.set("zookeeper.znode.parent", "/v09410-hbase");
	}
	public static void main(String[] args) throws IOException {
		
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();
		
		Collection<ServerName> collection = clusterStatus.getServers();
		Iterator<ServerName> iteratorServerName = collection.iterator();
		
		CompactionManager compactionManager = new CompactionManager(Integer.parseInt("3"), Integer.parseInt("1"), "zk1.mars.grid.sina.com.cn:2181,zk2.mars.grid.sina.com.cn:2181,zk3.mars.grid.sina.com.cn:2181,zk4.mars.grid.sina.com.cn:2181,zk5.mars.grid.sina.com.cn:2181",
			        "/v09410-hbase", "hdfs://h112158.mars.grid.sina.com.cn:9000", "/hbase", false);
		while(iteratorServerName.hasNext()){
			HServerLoad serverLoad = clusterStatus.getLoad(iteratorServerName.next());
			System.out.println("============= "+serverLoad.getStorefiles()+" =============" );
			Map <byte[],RegionLoad>  regionsLoad = serverLoad.getRegionsLoad();
		    Iterator<byte[]> iterator = regionsLoad.keySet().iterator();
		    
		    while (iterator.hasNext()) {
		    	RegionLoad regionLoad = regionsLoad.get(iterator.next());
		    	//compactionManager.getTableNameFromRegionName();
		    	System.out.println(regionLoad.getNameAsString()+"-------"+regionLoad.getStorefiles()+"-------"+regionLoad.getStores());
		    	//	System.out.println(regionLoad.);
			}
			
		}
	}
}
