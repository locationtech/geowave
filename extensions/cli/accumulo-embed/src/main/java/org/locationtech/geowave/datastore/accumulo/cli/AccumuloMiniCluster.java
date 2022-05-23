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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.monitor.Monitor;
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

  protected static boolean isYarn() {
    return VersionUtil.compareVersions(VersionInfo.getVersion(), "2.2.0") >= 0;
  }

  public static void main(final String[] args) throws Exception {

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

    MiniAccumuloUtils.setProperty(miniAccumuloConfig, Property.MONITOR_PORT, "9995");

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

    MiniAccumuloUtils.exec(accumulo, Monitor.class);

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
}
