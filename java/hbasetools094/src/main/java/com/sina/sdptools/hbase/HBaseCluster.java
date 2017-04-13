package com.sina.sdptools.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HBaseCluster {
	private HBaseAdmin admin;
	private HMasterInterface master;
	private List<RegionServer> regionServers;
	private Map<String, RegionServer> ipStr2RegionServers = Maps.newHashMap();
	private Map<String, List<HRegionInfo>> tablesRegionInfo;

	@SuppressWarnings("deprecation")
	public HBaseCluster(String zookeeper, String parentNode)
			throws MasterNotRunningException, ZooKeeperConnectionException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zookeeper);
		conf.set("zookeeper.znode.parent", parentNode);
		this.admin = new HBaseAdmin(conf);
		this.master = this.admin.getMaster();
		this.tablesRegionInfo = Maps.newHashMap();
	}

	public List<RegionServer> getClusterRegionServers() throws IOException {
		Collection<ServerName> servers = this.master.getClusterStatus()
				.getServers();
		List<RegionServer> list = Lists.newArrayList();
		this.ipStr2RegionServers = Maps.newHashMap();
		for (ServerName name : servers) {
			HServerLoad serverLoad = this.master.getClusterStatus().getLoad(
					name);
			Map<byte[], HServerLoad.RegionLoad> regionLoads = serverLoad
					.getRegionsLoad();
			//Map<String, String> metrics = getServerHTMLMetrics(name.getHostname());
			Map<String, String> metrics = null;
			RegionServer server = new RegionServer(name, serverLoad,
					regionLoads, metrics);
			list.add(server);

			this.ipStr2RegionServers.put(name.getHostname(), server);
		}
		return list;
	}

	public Map<String, RegionServer> getIpStr2RegionServers(boolean realtime)
			throws IOException {
		if (realtime) {
			getClusterRegionServers();
		}
		return this.ipStr2RegionServers;
	}

	public RegionServer getRegionServerByIP(String hostName) {
		return (RegionServer) this.ipStr2RegionServers.get(hostName);
	}

	public List<HRegionInfo> getHRegionInfoByTable(String tableName)
			throws IOException {
		List<HRegionInfo> regionInfos =  this.tablesRegionInfo
				.get(tableName);
		if (regionInfos == null) {
			regionInfos = this.admin.getTableRegions(Bytes.toBytes(tableName));
			this.tablesRegionInfo.put(tableName, regionInfos);
		}
		return regionInfos;
	}

	public Map<String, Map<byte[], HServerLoad.RegionLoad>> getTablesRegionLoads()
			throws IOException {
		Map<String, Map<byte[], HServerLoad.RegionLoad>> tablesRegionLoads = Maps
				.newHashMap();
		this.regionServers = getClusterRegionServers();
		int regionCount = this.admin.getClusterStatus().getRegionsCount();
		Map<byte[], HServerLoad.RegionLoad> regionLoads = Maps
				.newHashMapWithExpectedSize(regionCount);
		for (RegionServer rs : this.regionServers) {
			regionLoads.putAll(rs.getRegionLoads());
		}
		for (Map.Entry<byte[], HServerLoad.RegionLoad> entry : regionLoads
				.entrySet()) {
			String tableName = String.valueOf(entry.getKey()).split(",")[0];
			Map<byte[], HServerLoad.RegionLoad> regionLoadMap = tablesRegionLoads
					.get(tableName);
			if (regionLoadMap == null) {
				regionLoadMap = Maps.newHashMap();
				tablesRegionLoads.put(tableName, regionLoadMap);
			}
			regionLoadMap.put(entry.getKey(), entry.getValue());
		}
		return tablesRegionLoads;
	}

	public HBaseAdmin getAdmin() {
		return this.admin;
	}

	public HMasterInterface getMaster() {
		return this.master;
	}
}
