package com.sina.sdptools.core;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.PropertyConfigurator;

public class RunProgramDriver {

  Map<String, ProgramDescription> programs;

  public RunProgramDriver() {
    programs = new TreeMap<String, ProgramDescription>();
  }

  static private class ProgramDescription {

    static final Class<?>[] paramTypes = new Class<?>[] { String[].class };
    static final Class<?>[] paramTypesTool = new Class<?>[] { String.class,
        RunTool.class, String[].class, String.class };

    /**
     * Create a description of an example program.
     * 
     * @param mainClass
     *            the class with the main for the example program
     * @param description
     *            a string to display to the user in help messages
     */
    public ProgramDescription(String name, String mainClass,
        String description) {
      this.name = name;
      this.mainClass = mainClass;
      this.description = description;
    }

    /**
     * Invoke the example application with the given arguments
     * 
     * @param args
     *            the arguments for the application
     * @throws Throwable
     *             The exception thrown by the invoked method
     */
    public void invoke(String[] args) throws Throwable {
      try {
        ClassLoader classLoader = Thread.currentThread()
            .getContextClassLoader();
        if (classLoader == null) {
          classLoader = RunProgramDriver.class.getClassLoader();
        }
        Class<?> mainClazz = classLoader.loadClass(mainClass);
        Method main = null;
        try {
          main = mainClazz.getMethod("main", paramTypes);
          main.invoke(null, new Object[] { args });
        } catch (NoSuchMethodException e) {
          Class<?> clz = mainClazz.getSuperclass();
          if (clz != null && clz == RunTool.class) {
            main = mainClazz.getMethod("execTool", paramTypesTool);
            Object instance = mainClazz.newInstance();
            main.invoke(instance, new Object[] { name, instance,
                args, description });
          } else {
            System.out
                .println("main method is not found in class: "
                    + mainClass);
          }
        }
      } catch (InvocationTargetException except) {
        throw except.getCause();
      }
    }

    public String getDescription() {
      return description;
    }

    public String getMainClass() {
      return mainClass;
    }

    private String name;
    private String mainClass;
    private String description;
  }

  private static void printUsage(Map<String, ProgramDescription> programs) {
    System.out.println("Valid program names are:");
    for (Map.Entry<String, ProgramDescription> item : programs.entrySet()) {
      System.out.println("  " + item.getKey() + ": "
          + item.getValue().getDescription());
    }
  }

  /**
   * This is the method that adds the classed to the repository
   * 
   * @param name
   *            The name of the string you want the class instance to be
   *            called with
   * @param mainClass
   *            The class that you want to add to the repository
   * @param description
   *            The description of the class
   */
  public void addClass(String name, String mainClass, String description)
      throws Throwable {
    programs.put(name, new ProgramDescription(name, mainClass, description));
  }

  /**
   * This is a driver for the example programs. It looks at the first command
   * line argument and tries to find an example program with that name. If it
   * is found, it calls the main method in that class with the rest of the
   * command line arguments.
   * 
   * @param args
   *            The argument from the user. args[0] is the command to run.
   * @throws NoSuchMethodException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws Throwable
   *             Anything thrown by the example program's main
   */
  public void driver(String[] args) throws Throwable {
    // Make sure they gave us a program name.
    if (args.length == 0) {
      System.out.println("An example program must be given as the"
          + " first argument.");
      printUsage(programs);
      System.exit(-1);
    }

    // And that it is good.
    ProgramDescription pgm = programs.get(args[0]);
    if (pgm == null) {
      System.out.println("Unknown program '" + args[0] + "' chosen.");
      printUsage(programs);
      System.exit(-1);
    }
    initLog(args, pgm);

    // Remove the leading argument and call main
    String[] new_args = new String[args.length - 1];
    for (int i = 1; i < args.length; ++i) {
      new_args[i - 1] = args[i];
    }
    pgm.invoke(new_args);
  }

  private void initLog(String[] args, ProgramDescription pgm) {
    try {
      ClassLoader classLoader = Thread.currentThread()
          .getContextClassLoader();
      if (classLoader == null) {
        classLoader = RunProgramDriver.class.getClassLoader();
      }

      URL url = null;
      String path = null;
      for (String s : args) {
        if (s.startsWith("-Dlog4j.configuration=")) {
          path = s.substring("-Dlog4j.configuration=".length());
          url = getResource(classLoader, path);
        }
      }
      if (url == null) {
        String str = pgm.getMainClass();
        str = str.substring(str.lastIndexOf('.') + 1);
        path = "conf/" + str + "_log4j.properties";
        url = getResource(classLoader, path);
      }

      if (url == null) {
        path = "conf/log4j.properties";
        url = getResource(classLoader, path);
      }

      if (url != null) {
        PropertyConfigurator.configure(url);
      }
    } catch (Exception e) {
      // nothing to do.
    }
  }

  private URL getResource(ClassLoader classLoader, String path)
      throws MalformedURLException {
    URL url;
    File f = new File(path);
    if (f.exists() && f.isFile()) {
      url = f.toURI().toURL();
    } else {
      url = classLoader.getResource(path);
    }
    return url;
  }

}
