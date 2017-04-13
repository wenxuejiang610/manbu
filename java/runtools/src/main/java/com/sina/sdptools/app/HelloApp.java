package com.sina.sdptools.app;

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sina.sdptools.core.RunTool;

public class HelloApp extends RunTool {
  private static Logger log = LoggerFactory.getLogger(HelloApp.class);

  @Override
  protected int getArgumentNumber() {
    return 1;
  }

  @Override
  protected void printHelp(PrintStream out) {
    printCmdHelp(out, "-echo <msg>", new String[] { "-Drepeate=1" },
        "Echo the message for repeate times");
    printCmdHelp(out, "-bye", new String[] {}, "Say \"Bye!\"");
  }

  @Override
  public int exec(String[] args) throws Exception {
    Configuration conf = getConf();
    String cmd = args[0];
    if ("-echo".equalsIgnoreCase(cmd)) {
      return echo(conf, args);
    } else if ("-bye".equalsIgnoreCase(cmd)) {
      return bye(conf, args);
    } else {
      log.info("unknown command");
      return printHelp();
    }
  }

  private int bye(Configuration conf, String[] args) {
    log.info("execute method(bye)");
    System.out.println("Good bye!");
    return 0;
  }

  private int echo(Configuration conf, String[] args) {
    log.info("execute method(echo)");
    if (args.length < 2) {
      return printHelp();
    }
    int times = conf.getInt("repeate", 1);
    for (int i = 0; i < times; i++) {
      System.out.println("Hello: " + args[1]);
    }
    return 0;
  }

}

