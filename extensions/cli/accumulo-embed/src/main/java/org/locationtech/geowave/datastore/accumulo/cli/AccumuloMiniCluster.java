/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.Files;

public class AccumuloMiniCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloMiniCluster.class);
  private static final String DEFAULT_LIB_DIR = "lib/services/third-party/embedded-accumulo/lib";
  private static final String JAVA_TOOL_OPTIONS = "JAVA_TOOL_OPTIONS";
  private static final String IGNORE_UNRECOGNIZED_VM_OPTIONS = "-XX:+IgnoreUnrecognizedVMOptions";

  protected static boolean isYarn() {
    return VersionUtil.compareVersions(VersionInfo.getVersion(), "2.2.0") >= 0;
  }

  public static void main(final String[] args) throws Exception {
    final String toolOptions = System.getenv(JAVA_TOOL_OPTIONS);
    if ((toolOptions == null) || !toolOptions.contains(IGNORE_UNRECOGNIZED_VM_OPTIONS)) {
      final int exitCode = relaunchIgnoringUnrecognizedVmOptions(args, toolOptions);
      if (exitCode != 0) {
        System.exit(exitCode);
      }
      return;
    }

    Configurator.setLevel(LogManager.getRootLogger().getName(), Level.WARN);

    final boolean interactive =
        (System.getProperty("interactive") != null)
            ? Boolean.parseBoolean(System.getProperty("interactive"))
            : true;

    final String password = System.getProperty("password", "secret");
    final String user = System.getProperty("rootUser", "root");

    final File tempDir = Files.createTempDir();
    final String instanceName = System.getProperty("instanceName", "accumulo");
    final MiniAccumuloConfig miniAccumuloConfig =
        new MiniAccumuloConfig(tempDir, password).setNumTservers(2).setInstanceName(
            instanceName).setZooKeeperPort(2181);

    MiniAccumuloUtils.setRootUserName(miniAccumuloConfig, user);
    // MiniAccumulo waits for its ZooKeeper to answer "ruok", which ZooKeeper 3.5+ ignores unless
    // whitelisted; these properties are passed to every process the cluster launches
    MiniAccumuloUtils.setSystemProperties(
        miniAccumuloConfig,
        Collections.singletonMap("zookeeper.4lw.commands.whitelist", "ruok"));

    final String geowaveHome =
        System.getProperty("geowave.home", DataStoreUtils.DEFAULT_GEOWAVE_DIRECTORY);
    final File libDir = new File(geowaveHome, DEFAULT_LIB_DIR);
    final URL[] extraLibraries;
    if (libDir.exists() && libDir.isDirectory()) {
      extraLibraries =
          Arrays.stream(
              libDir.listFiles(
                  (f) -> f.isFile() && f.getName().toLowerCase().endsWith(".jar"))).map(f -> {
                    try {
                      return f.toURI().toURL();
                    } catch (final MalformedURLException e) {
                      LOGGER.warn("Unable to add to accumulo classpath", e);
                    }
                    return null;
                  }).filter(Objects::nonNull).toArray(URL[]::new);
    } else {
      extraLibraries = new URL[0];
    }
    final MiniAccumuloCluster accumulo =
        MiniAccumuloClusterFactory.newAccumuloCluster(
            miniAccumuloConfig,
            AccumuloMiniCluster.class,
            extraLibraries);
    accumulo.start();

    System.out.println("starting up ...");
    Thread.sleep(3000);

    System.out.println(
        "cluster running with root user "
            + user
            + ", password "
            + password
            + ", instance name "
            + accumulo.getInstanceName()
            + ", and zookeeper "
            + accumulo.getZooKeepers());

    if (interactive) {
      System.out.println("Press Enter to shutdown..");
      System.in.read();
      System.out.println("Shutting down!");
      accumulo.stop();
    } else {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            accumulo.stop();
          } catch (final Exception e) {
            LOGGER.warn("Unable to shutdown Accumulo", e);
            System.out.println("Error shutting down Accumulo.");
          }
          System.out.println("Shutting down!");
        }
      });

      while (true) {
        Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
      }
    }
  }

  /**
   * Accumulo 2.0's MiniAccumuloCluster puts -XX:+UseConcMarkSweepGC on the command line of every
   * process it starts, and there is no hook to change that. CMS was removed in Java 14, so none of
   * them start. Child processes inherit JAVA_TOOL_OPTIONS, which is read before the command line,
   * so run the cluster from a copy of this JVM whose environment tells the JVM to ignore the flag.
   */
  private static int relaunchIgnoringUnrecognizedVmOptions(
      final String[] args,
      final String toolOptions) throws IOException, InterruptedException {
    final List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    // system properties carry this class's settings; other options (e.g. a debug agent's port)
    // would clash with the running JVM
    ManagementFactory.getRuntimeMXBean().getInputArguments().stream().filter(
        a -> a.startsWith("-D")).forEach(command::add);
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(AccumuloMiniCluster.class.getName());
    command.addAll(Arrays.asList(args));

    final ProcessBuilder builder = new ProcessBuilder(command).inheritIO();
    builder.environment().put(
        JAVA_TOOL_OPTIONS,
        toolOptions == null ? IGNORE_UNRECOGNIZED_VM_OPTIONS
            : toolOptions + " " + IGNORE_UNRECOGNIZED_VM_OPTIONS);
    final Process process = builder.start();
    final Thread stopChild = new Thread(process::destroy);
    Runtime.getRuntime().addShutdownHook(stopChild);
    final int exitCode = process.waitFor();
    Runtime.getRuntime().removeShutdownHook(stopChild);
    return exitCode;
  }
}
