package com.sina.sdptools.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.Maps;

public class RegionServer {
  ServerName servername;
  ServerLoad serverLoad;
  Map<byte[], RegionLoad> regionLoads;
  Map<String, String> metrics;

  public RegionServer(ServerName servername, ServerLoad load, Map<byte[], RegionLoad> serverRegions) {
    this.servername = servername;
    this.serverLoad = load;
    this.regionLoads = serverRegions;
  }

  public Map<byte[], RegionLoad> getRegionLoads(List<HRegionInfo> tableRegions) throws IOException {
    Map<byte[], RegionLoad> regionLoads = Maps.newHashMap();
    for (HRegionInfo region : tableRegions) {
      if (regionLoads.containsKey(region.getRegionName())) {
        regionLoads.put(region.getRegionName(), regionLoads.get(region.getRegionName()));
      }
    }
    return regionLoads;
  }

  public ServerLoad getServerLoad() {
    return this.serverLoad;
  }

  public Map<byte[], RegionLoad> getRegionLoads() {
    return this.regionLoads;
  }

  public ServerName getServername() {
    return this.servername;
  }

  public String getIP() {
    return this.servername.getHostname();
  }
}