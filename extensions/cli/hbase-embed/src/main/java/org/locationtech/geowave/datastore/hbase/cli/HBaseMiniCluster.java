/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelService;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelServiceManager;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.locationtech.geowave.datastore.hbase.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMiniCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMiniCluster.class);

  protected String zookeeper;

  private final String zkDataDir;

  private Object hbaseLocalCluster;
  private final String hbaseLibDir;
  private final String hbaseDataDir;
  private final int numRegionServers;

  public HBaseMiniCluster(
      final List<String> auths,
      final String zkDataDir,
      final String hbaseLibDir,
      final String hbaseDataDir,
      final int numRegionServers) {
    this.auths = auths;
    this.zkDataDir = zkDataDir;
    this.hbaseLibDir = hbaseLibDir;
    this.hbaseDataDir = hbaseDataDir;
    this.numRegionServers = numRegionServers;
  }

  // VisibilityTest valid authorizations
  private final List<String> auths;

  protected User SUPERUSER;

  public void setup() {
    if (hbaseLocalCluster == null) {
      if ((zookeeper == null) || zookeeper.isEmpty()) {
        zookeeper = ZookeeperMiniCluster.getInstance(hbaseLibDir, zkDataDir).getZookeeper();
        LOGGER.debug("Using local zookeeper URL: " + zookeeper);
      }

      final ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
      final ClassLoader hbaseMiniClusterCl =
          HBaseMiniClusterClassLoader.getInstance(prevCl, hbaseLibDir);
      Thread.currentThread().setContextClassLoader(hbaseMiniClusterCl);
      try {
        final Configuration conf =
            (Configuration) Class.forName(
                "org.apache.hadoop.hbase.HBaseConfiguration",
                true,
                hbaseMiniClusterCl).getMethod("create").invoke(null);
        System.setProperty("test.build.data.basedirectory", hbaseDataDir);
        conf.setBoolean("hbase.online.schema.update.enable", true);
        conf.setBoolean("hbase.defaults.for.version.skip", true);
        conf.setIfUnset("hbase.root.dir", hbaseDataDir);
        if (zookeeper != null && zookeeper.contains(":")) {
          conf.setIfUnset("zookeeper.host", zookeeper.split(":")[0]);
          conf.setIfUnset("zookeeper.port", zookeeper.split(":")[1]);
          conf.setIfUnset("zookeeper.connection.string", zookeeper);
          conf.setIfUnset("hbase.zookeeper.quorum", "localhost");
          conf.set("hbase.zookeeper.property.clientPort", zookeeper.split(":")[1]);
        }
        final boolean enableVisibility = (auths != null) && !auths.isEmpty();
        if (enableVisibility) {
          conf.set("hbase.superuser", "admin");

          conf.setBoolean("hbase.security.authorization", true);

          conf.setBoolean("hbase.security.visibility.mutations.checkauths", true);
          // setup vis IT configuration
          conf.setClass(
              VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS,
              SimpleScanLabelGenerator.class,
              ScanLabelGenerator.class);

          conf.setClass(
              VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
              HBaseTestVisibilityLabelServiceImpl.class,
              VisibilityLabelService.class);

          // Install the VisibilityController as a system
          // processor
          VisibilityTestUtil.enableVisiblityLabels(conf);
        }

        // HBaseTestingUtility must be loaded dynamically by the
        // minicluster class loader
        hbaseLocalCluster =
            Class.forName(
                // "org.apache.hadoop.hbase.HBaseTestingUtility",
                "org.locationtech.geowave.datastore.hbase.cli.GeoWaveHBaseUtility",
                true,
                hbaseMiniClusterCl).getConstructor(Configuration.class).newInstance(conf);

        // Start the cluster
        hbaseLocalCluster.getClass().getMethod(
            "startMiniHBaseCluster",
            Integer.TYPE,
            Integer.TYPE).invoke(hbaseLocalCluster, 1, numRegionServers);


        if (enableVisibility) {
          // Set valid visibilities for the vis IT
          final Connection conn = ConnectionPool.getInstance().getConnection(zookeeper);
          try {
            SUPERUSER = User.createUserForTesting(conf, "admin", new String[] {"supergroup"});

            // Set up valid visibilities for the user
            addLabels(
                conn.getConfiguration(),
                auths.toArray(new String[0]),
                User.getCurrent().getName());

          } catch (final Throwable e) {
            LOGGER.error("Error creating test user", e);
          }
        }
      } catch (final Exception e) {
        LOGGER.error("Exception starting hbaseLocalCluster", e);
      }
      Thread.currentThread().setContextClassLoader(prevCl);
    }
  }

  private void addLabels(final Configuration conf, final String[] labels, final String user)
      throws Exception {
    final PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
          @Override
          public VisibilityLabelsResponse run() throws Exception {
            try {
              VisibilityClient.addLabels(conf, labels);

              VisibilityClient.setAuths(conf, labels, user);
            } catch (final Throwable t) {
              throw new IOException(t);
            }
            return null;
          }
        };

    SUPERUSER.runAs(action);
  }

  public void tearDown() {
    if (hbaseLocalCluster != null) {
      try {
        hbaseLocalCluster.getClass().getMethod("shutdownMiniCluster").invoke(hbaseLocalCluster);
        if (!(Boolean) hbaseLocalCluster.getClass().getMethod("cleanupTestDir").invoke(
            hbaseLocalCluster)) {
          LOGGER.warn("Unable to delete mini hbase temporary directory");
        }
        hbaseLocalCluster = null;
      } catch (final Exception e) {
        LOGGER.warn("Unable to shutdown and delete mini hbase temporary directory", e);
      }
    }
  }

}
