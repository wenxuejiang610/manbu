package com.sina.sdptools.app;

import java.io.PrintStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.core.RunTool;
import com.sina.sdptools.hadoop.job.tool.JobManager;

public class JobCleanerApp extends RunTool {
  private Logger log = LoggerFactory.getLogger(getClass().getSimpleName());

  @Override
  protected void printHelp(PrintStream out) {
    printCmdHelp(out, "", new String[] {}, "Usage: jobTrackerIp, jobTrackerPort,overtime,factor");
    printCmdHelp(out, "", new String[] {}, "eg: 10.13.0.163 9001 7200000 3");
  }

  @Override
  public int exec(String[] arg0) {
    log.info("This app for hadoop 0.20.2 job checker");
    String ip = arg0[0];
    String port = arg0[1];
    String overtime = arg0[2];
    String str_factor = arg0[3];
    try {
      int factor = Integer.parseInt(str_factor);
      JobManager jm = new JobManager(ip, port);
      List<String> oj = jm.listOvertimeJobs(Long.parseLong(overtime));
      for (String jobid : oj) {
        jm.killMapTask(jobid, factor);
        jm.killReduceTask(jobid, factor);
      }
    }
    catch (final Exception e) {
      log.error(e.getMessage());
      return 1;
    }
    return 0;
  }

  @Override
  protected int getArgumentNumber() {
    return 4;
  }

}
