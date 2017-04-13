package com.sina.sdptools.app.hfilesync.core;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShellUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ShellUtils.class);

    private ShellUtils() {}

    public static void die(String message) {
        LOG.error(message);
        System.exit(1);
    }

    public static void die(String message, Throwable reason) {
        LOG.error(message, reason);
        System.exit(1);
    }

    /** 调用 {@code tool}，结束后退出进程。*/
    public static void run(Tool tool, String[] args) {
        try {
            int exitCode = ToolRunner.run(tool, args);
            if (exitCode == 0) {
                LOG.info("Done.");
            } else {
                LOG.info("Exit with error code " + exitCode + ".");
            }
            System.exit(exitCode);
        } catch (Throwable e) {
            die("Caught exception while running " + tool.getClass().getSimpleName(), e);
        }
    }
}
