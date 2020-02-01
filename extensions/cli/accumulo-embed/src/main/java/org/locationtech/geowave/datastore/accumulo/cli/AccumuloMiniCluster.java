/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.monitor.Monitor;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
import com.google.common.io.Files;

public class AccumuloMiniCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloMiniCluster.class);

  protected static boolean isYarn() {
    return VersionUtil.compareVersions(VersionInfo.getVersion(), "2.2.0") >= 0;
  }

  public static void main(final String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);

    final boolean interactive =
        (System.getProperty("interactive") != null)
            ? Boolean.parseBoolean(System.getProperty("interactive"))
            : true;

    final String password = System.getProperty("password", "secret");
    final String user = System.getProperty("rootUser", "root");

    final File tempDir = Files.createTempDir();
    final String instanceName = System.getProperty("instanceName", "accumulo");
    final MiniAccumuloConfigImpl miniAccumuloConfig =
        new MiniAccumuloConfigImpl(tempDir, password).setNumTservers(2).setInstanceName(
            instanceName).setZooKeeperPort(2181);

    miniAccumuloConfig.setRootUserName(user);

    miniAccumuloConfig.setProperty(Property.MONITOR_PORT, "9995");

    final MiniAccumuloClusterImpl accumulo =
        MiniAccumuloClusterFactory.newAccumuloCluster(
            miniAccumuloConfig,
            AccumuloMiniCluster.class);
    accumulo.start();

    accumulo.exec(Monitor.class);

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
