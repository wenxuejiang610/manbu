package com.sina.sdptools.core;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RunTool extends Configured implements Tool {
  private Logger log = LoggerFactory.getLogger(getClass());

  public static final String KEY_PREFIX = "sina.";
  public static final String KEY_VERSION = KEY_PREFIX + "version";
  public static final String KEY_TOOL_ID = KEY_PREFIX + "tool.id";
  public static final String KEY_JOB_START_TIME = KEY_PREFIX
      + "job.start.time";
  public static final DateFormat dateFormat = new SimpleDateFormat(
      "yyMMddHHmmss");

  protected String toolId;

  protected String cmdTag;

  protected static String lineSep = "\n";

  public String getToolId() {
    return toolId;
  }

  // --------------help
  /**
   * 设置必须输入的参数个数.
   * 
   * @param argNum
   */
  protected abstract int getArgumentNumber();

  public final int printHelp() {
    System.out.println("Usage: " + toolId);
    printHelp(System.out);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  protected final void printCmdHelp(PrintStream out, String cmd) {
    printCmdHelp(out, cmd, null, null);
  }

  protected final void printCmdHelp(PrintStream out, String cmd,
      String description) {
    printCmdHelp(out, cmd, null, description);
  }

  protected final void printCmdHelp(PrintStream out, String cmd,
      String[] options, String description) {
    StringBuilder sb = new StringBuilder();
    sb.append(toolId);
    if (options != null && options.length != 0) {
      for (String str : options) {
        sb.append(" ").append(str);
      }
    }
    sb.append(" ").append(cmd);
    if (!StringUtils.isEmpty(description)) {
      sb.append(" :").append(description);
    }
    out.println(sb.toString());
  }

  protected void printHelp(PrintStream out) {
  }

  protected void printCmdArgHelp(String msg) {
    System.out.println(toolId + " " + msg);
    System.out.println();
    System.out.println("User general options:");
    printOptArgHelp("mapred.job.name=" + toolId + "-${job.start.time}",
        "job name");
  }

  protected void printOptArgHelp(String arg, String desc) {
    System.out.print(" -D");
    System.out.print(arg);
    System.out.print("    -");
    System.out.println(desc);
  }

  // -----------------execute
  public void execTool(String toolId, RunTool tool, String[] args,
      String description) {
    DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    long begts = System.currentTimeMillis();
    String tag = df.format(new Date(begts));
    log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~S" + tag);
    try {
      log.info("Tool(" + toolId + ") execute begin.");
      if (args == null) {
        args = new String[0];
      }
      StringBuilder sb = new StringBuilder();
      sb.append("Desc: ").append(description).append("\n");
      sb.append("Class: ").append(tool.getClass().getName()).append("\n");
      sb.append("Args: ").append(toolId);
      if (args.length > 0) {
        for (int i = 0; i < args.length; i++) {
          sb.append(" ").append(args[i]);
        }
      }
      log.info(sb.toString());

      // check
      tool.setToolId(toolId);
      tool.setCommandTag(sb.toString());
      Configuration conf = tool.getConf();
      if (conf == null) {
        conf = new Configuration();
      }
      addRunConf(conf, tool);
      processVersionInfo(conf);

      ToolRunner.run(conf, tool, args);
      log.info("Tool(" + toolId + ") execute ok.");
    } catch (Throwable t) {
      log.error("Tool(" + toolId + ") execute failed.", t);
    } finally {
      log.info("execute time: "
          + ((System.currentTimeMillis() - begts) / 1000) + "s");
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~E" + tag + "\n");
    }
  }

  @Override
  public final int run(String[] args) throws Exception {
    // for (int i = 0; i < args.length; i++) {
    // log.info("arg[" + i + "]: " + args[i]);
    // }
    if (args.length < getArgumentNumber()) {
      System.out
          .println("[ERROR]: Some required arguments is not specified!");
      printHelp();
      log.info("Tool(" + toolId + ") print help ok.");
      return -1;
    }

    Configuration conf = this.getConf();
    // 处理文件输入输出格式简洁设置
    String str = conf.get("mapreduce.inputformat.class");
    if (StringUtils.isNotEmpty(str) && str.indexOf(".") == -1) {
      str = "org.apache.hadoop.mapreduce.lib.input." + str;
      conf.set("mapreduce.inputformat.class", str);
    }
    str = conf.get("mapreduce.outputformat.class");
    if (StringUtils.isNotEmpty(str) && str.indexOf(".") == -1) {
      str = "org.apache.hadoop.mapreduce.lib.output." + str;
      conf.set("mapreduce.outputformat.class", str);
    }

    return exec(args);
  }

  public abstract int exec(String[] args) throws Exception;

  public String getCommandTag() {
    Configuration conf = getConf();
    long t = conf.getLong(KEY_JOB_START_TIME, -1);
    if (t < 0) {
      return this.cmdTag;
    }
    long now = System.currentTimeMillis();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    return this.cmdTag + "\nStarted at: " + df.format(new Date(t))
        + "\nFinished at: " + df.format(new Date(now))
        + "\nFinished in: " + ((now - t) / 1000) + "s";
  }

  private void setCommandTag(String cmdTag) {
    this.cmdTag = cmdTag;
  }

  private void setToolId(String toolId) {
    this.toolId = toolId;
  }

  private void addRunConf(Configuration conf, RunTool tool) {
    conf.addResource("conf/run-default.xml");
    conf.addResource("tool/" + tool.getClass().getSimpleName() + ".xml");
    conf.addResource(tool.getClass().getSimpleName() + "-site.xml");
    conf.addResource("run-site.xml");

    conf.setIfUnset(KEY_JOB_START_TIME,
        String.valueOf(System.currentTimeMillis()));
    conf.setIfUnset(KEY_TOOL_ID, tool.getToolId());

    // 设置作业名称
    String str = conf.get("mapred.job.name", tool.getToolId()
        + "-${job.start.time}");
    if (str.indexOf('$') != -1) {
      String tag = dateFormat.format(new Date(conf.getLong(
          KEY_JOB_START_TIME, System.currentTimeMillis())));
      str = str.replaceAll("\\$\\{job.start.time\\}", tag);
      conf.setIfUnset("mapred.job.name", str);
    }
  }

  private void processVersionInfo(Configuration conf) {
    StringBuilder sb = new StringBuilder();
    for (String line : VersionInfo.versionReport()) {
      sb.append("; ").append(line);
    }
    conf.set(KEY_VERSION, sb.substring(1));
    // VersionInfo.logVersion();
  }


  // -------------------util
  public void dynaSetReduceTasks(Job job) {
    Configuration conf = job.getConfiguration();
    try {
      long ratio = conf.getLong("mapred.reduce.tasks.dyna.bytesRatio",
          2147483648L);
      log.info("<conf> mapred.reduce.tasks.dyna.bytesRatio=" + ratio);
      int maxNumReduceTasks = conf.getInt("mapred.reduce.tasks.max", 10);
      int minNumReduceTasks = conf.getInt("mapred.reduce.tasks.min", 1);
      log.info("<conf> mapred.reduce.tasks.min=" + minNumReduceTasks);
      log.info("<conf> mapred.reduce.tasks.max=" + maxNumReduceTasks);

      @SuppressWarnings("rawtypes")
      InputFormat fif = job.getInputFormatClass().newInstance();
      @SuppressWarnings("unchecked")
      List<InputSplit> splitList = fif.getSplits(job);
      long sum = 0;
      for (InputSplit is : splitList) {
        sum += is.getLength();
      }
      int tasks = (int) (sum / ratio);
      long diff = sum % ratio;
      if (diff != 0 && (ratio / diff < 4)) {
        tasks++;
      }
      if (tasks > maxNumReduceTasks) {
        tasks = maxNumReduceTasks;
      }
      if (tasks < minNumReduceTasks) {
        tasks = minNumReduceTasks;
      }
      job.setNumReduceTasks(tasks);
      log.info("mapred.reduce.tasks=" + tasks + "(sum=" + sum
          + ", splits=" + splitList.size() + ")");
    } catch (Exception e) {
      log.error("dynaSetReduceTasks failed", e);
    }
  }

  public final Path newJobTempPath() {
    Configuration conf = getConf();
    String str = conf.get("mapred.job.name");
    if (str == null) {
      long ts = conf.getLong(KEY_JOB_START_TIME,
          System.currentTimeMillis());
      str = toolId + "_" + dateFormat.format(new Date(ts));
    }
    str = "/run/temp/" + str + "_temp";
    conf.set("run.mapred.tmp.dir", str);
    return new Path(str);
  }

  public final void updateConf(Configuration conf, String name, String name2,
      String defaultValue) {
    String value = conf.get(name2);
    if (value != null) {
      conf.set(name, value);
      return;
    }
    value = conf.get(name);
    if (value == null) {
      conf.set(name, defaultValue);
    }
  }

  public final void printCounters(Job job, StringBuffer sb)
      throws IOException {
    // 输出计数器值
    Counters counters = job.getCounters();
    sb.append("Counters: ").append(counters.countCounters())
        .append(lineSep);
    for (String groupName : counters.getGroupNames()) {
      sb.append("  ").append(groupName).append(lineSep);
      CounterGroup cg = counters.getGroup(groupName);
      Iterator<Counter> iter = cg.iterator();
      while (iter.hasNext()) {
        Counter counter = iter.next();
        sb.append("    ").append(counter.getDisplayName()).append('=')
            .append(counter.getValue()).append(lineSep);
      }
    }
  }

  public final void writeJobLog(Job job) throws IOException {
    Configuration conf = job.getConfiguration();
    Path resultFile = new Path(conf.get("mapred.output.dir"), "result.txt");
    FileSystem fs = FileSystem.get(conf);
    PrintStream out = newPrintStream(fs.create(resultFile, true));
    try {
      out.println(this.getCommandTag());
      out.println("JobID: " + job.getJobID().toString());
      out.println("JobName: " + job.getJobName());
      out.println("TrackingURL: " + job.getTrackingURL());
      out.println("~~~~~~~~~~~~~~~~~~~~~~~");
      Iterator<CounterGroup> cgIter = job.getCounters().iterator();
      while (cgIter.hasNext()) {
        CounterGroup cg = cgIter.next();
        Iterator<Counter> cIter = cg.iterator();
        while (cIter.hasNext()) {
          Counter c = cIter.next();
          out.println(cg.getDisplayName() + " / "
              + c.getDisplayName() + " = " + c.getValue());
        }
      }
    } finally {
      out.close();
    }
  }

  public final PrintStream newPrintStream(OutputStream out) {
    PrintStream pout;
    try {
      if (!(out instanceof BufferedOutputStream)) {
        out = new BufferedOutputStream(out);
      }
      pout = new PrintStream(out, false, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.warn("Unsupported UTF-8 encode.", e);
      pout = new PrintStream(out, false);
    }
    return pout;
  }

  public final static String getInputSplitPath(Logger log, InputSplit split)
      throws IOException, InterruptedException {
    Class<? extends InputSplit> splitClass = split.getClass();
    InputSplit splitNew = split;
    if (splitClass.getName().equals(
        "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
      log.info("<TaggedInputSplit>: " + split.toString());
      // begin reflection hackery...
      try {
        Method getInputSplitMethod = splitClass
            .getDeclaredMethod("getInputSplit");
        getInputSplitMethod.setAccessible(true);
        splitNew = (InputSplit) getInputSplitMethod.invoke(split);
      } catch (Exception e) {
        // wrap and re-throw error
        throw new IOException(e);
      }
      // end reflection hackery
    }

    log.info("<InputSplit>: " + splitNew.toString());
    log.info("<InputSplit>: length = " + splitNew.getLength());
    String[] locations = splitNew.getLocations();
    for (int i = 0; i < locations.length; i++) {
      log.info("<InputSplit>: location[" + i + "] = " + locations[i]);
    }

    String path;
    if (splitNew instanceof FileSplit) {
      path = ((FileSplit) splitNew).getPath().toString();
    } else {
      path = "$nullpath$";
      log.info("<InputSplit>: path = " + path);
    }
    return path;

  }

  public void setInOutPaths(Job job, String[] args) throws IOException {
    if (job.getConfiguration().getBoolean("mixinputs", false)) {
      String[] paths = args[0].split(",");
      List<String> inputs = new ArrayList<String>();
      for (String p : paths) {
        inputs.add(p);
      }
      for (int i = 1; i < args.length - 1; i++) {
        inputs.add(args[i]);
      }
/**
      try {
        Class<? extends Mapper<?, ?, ?, ?>> mapper = job
            .getMapperClass();
        for (String p : inputs) {
          if (p.endsWith(".bcp")) {
            MultipleInputs.addInputPath(job, new Path(p),
                TextInputFormat.class);
            log.info("addInputPath: " + p);
          } else {
            MultipleInputs.addInputPath(job, new Path(p),
                SequenceFileInputFormat.class, mapper);
            log.info("addInputPath: " + p);
          }
        }
      } catch (ClassNotFoundException e) {
        throw new IOException("mapper", e);
      }
      */
    } else {
      FileSystem fs = FileSystem.get(job.getConfiguration());
      if (args.length > 2) {
        for (int i = 0; i < args.length - 1; i++) {
          Path p = new Path(args[i]);
          if (fs.globStatus(p).length == 0) {
            log.warn("addInputPath: not files - " + p);
            continue;
          }
          FileInputFormat.addInputPath(job, p);
          log.info("addInputPath: " + p);
        }
      } else if (args.length == 2) {
        // FileInputFormat.setInputPaths(job, args[0]);
        // log.info("addInputPath: " + args[0]);
        for (String s : args[0].split(",")) {
          Path p = new Path(s);
          if (fs.globStatus(p).length == 0) {
            log.warn("addInputPath: not files - " + p);
            continue;
          }
          FileInputFormat.addInputPath(job, p);
          log.info("addInputPath: " + s);
        }
      } else {
        throw new IOException("output path must be set!");
      }
    }
    Path outPath = new Path(args[args.length - 1]);
    FileOutputFormat.setOutputPath(job, outPath);
    log.info("setOutputPath: " + outPath);
  }
}
