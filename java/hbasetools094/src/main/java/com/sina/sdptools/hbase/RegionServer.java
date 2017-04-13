package com.sina.sdptools.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.Maps;

public class RegionServer {
  ServerName servername;
  HServerLoad serverLoad;
  Map<byte[], HServerLoad.RegionLoad> regionLoads;
  Map<String, String> metrics;

  public RegionServer(ServerName servername, HServerLoad load, Map<byte[], HServerLoad.RegionLoad> serverRegions,
      Map<String, String> metrics) {
    this.servername = servername;
    this.serverLoad = load;
    this.regionLoads = serverRegions;
    this.metrics = metrics;
  }

  public String getMetric(String name) {
    return (String) this.metrics.get(name);
  }

  public Map<byte[], HServerLoad.RegionLoad> getRegionLoads(List<HRegionInfo> tableRegions) throws IOException {
    Map<byte[], HServerLoad.RegionLoad> regionLoads = Maps.newHashMap();
    for (HRegionInfo region : tableRegions) {
      if (regionLoads.containsKey(region.getRegionName())) {
        regionLoads.put(region.getRegionName(), regionLoads.get(region.getRegionName()));
      }
    }
    return regionLoads;
  }

  public HServerLoad getServerLoad() {
    return this.serverLoad;
  }

  public Map<byte[], HServerLoad.RegionLoad> getRegionLoads() {
    return this.regionLoads;
  }

  public ServerName getServername() {
    return this.servername;
  }

  public String getIP() {
    return this.servername.getHostname();
  }
}