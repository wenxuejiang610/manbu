package com.sina.sdptools.core;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionInfo {
  private static Logger log = LoggerFactory.getLogger(VersionInfo.class);

  private static Properties data;

  public static String getProperty(String key) {
    if (data == null) {
      data = new Properties();
//      URL url = BcpUtils.getResource("version.properties");
//      if (url != null) {
//        try {
//          InputStream is = url.openStream();
//          data.load(is);
//          is.close();
//        } catch (IOException e) {
//          log.error("load version info file failed", e);
//        }
//      }
    }
    String value = data.getProperty(key);
    if (value == null || value.trim().length() == 0) {
      return "Unknown";
    }
    return value.trim();
  }

  public static String[] versionReport() {
    return new String[] {
        "BCPIMP " + getProperty("version"),
        "Subversion " + getProperty("url") + " -r "
            + getProperty("revision"),
        "Compiled by " + getProperty("user") + " on "
            + getProperty("date") };
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      log.info(line);
    }
  }

  public static void writeTo(PrintWriter out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void writeTo(PrintStream out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

}

