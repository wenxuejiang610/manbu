package com.sina.sdptools;

import com.sina.sdptools.core.RunProgramDriver;

public class AppDriver {

  public static void main(String[] args) {
    int exitCode = -1;
    RunProgramDriver pgd = new RunProgramDriver();
    try {
      // program---------------------
      pgd.addClass("hello", "com.sina.sdptools.app.HelloApp", "[APP]Test Freamwork");
      pgd.addClass("compaction", "com.sina.sdptools.app.CompactionApp", "[APP]Compation Tools");
      pgd.addClass("jobcleaner", "com.sina.sdptools.app.JobCleanerApp", "[APP]hadoop job cleaner tools");
      pgd.addClass("healthTest", "com.sina.sdptools.app.ClusterHealthCheckApp", "[APP]HBase health check tools");
      pgd.addClass("verfiy", "com.sina.sdptools.app.VerifyTabledataApp", "[APP]HBase 0.96 verfiy data tools");
      pgd.driver(args);
      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace(System.out);
    }
    System.exit(exitCode);
  }

}

