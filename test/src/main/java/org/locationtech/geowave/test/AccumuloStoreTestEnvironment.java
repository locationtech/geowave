/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import org.locationtech.geowave.datastore.accumulo.cli.MiniAccumuloClusterFactory;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AccumuloStoreTestEnvironment extends StoreTestEnvironment {
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new AccumuloStoreFactoryFamily().getDataStoreFactory();
  private static AccumuloStoreTestEnvironment singletonInstance = null;

  public static synchronized AccumuloStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new AccumuloStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStoreTestEnvironment.class);
  private static final boolean KEEP_LOGS = false;
  private static final int NUM_TABLET_SERVERS = 2;

  protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
  // breaks on windows if temp directory isn't on same drive as project
  protected static final File TEMP_DIR = new File("./target/accumulo_temp");
  protected String zookeeper;
  protected String accumuloInstance;
  protected String accumuloUser;
  protected String accumuloPassword;
  protected MiniAccumuloClusterImpl miniAccumulo;

  private final List<Process> cleanup = new ArrayList<>();

  private AccumuloStoreTestEnvironment() {}

  @Override
  public void setup() {

    if (!TestUtils.isSet(zookeeper)) {
      zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

      if (!TestUtils.isSet(zookeeper)) {
        zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
        LOGGER.debug("Using local zookeeper URL: " + zookeeper);
      }
    }

    if (!TestUtils.isSet(accumuloInstance)
        || !TestUtils.isSet(accumuloUser)
        || !TestUtils.isSet(accumuloPassword)) {

      accumuloInstance = System.getProperty("instance");
      accumuloUser = System.getProperty("username");
      accumuloPassword = System.getProperty("password");
      if (!TestUtils.isSet(accumuloInstance)
          || !TestUtils.isSet(accumuloUser)
          || !TestUtils.isSet(accumuloPassword)) {
        try {
          if (!TEMP_DIR.exists()) {
            if (!TEMP_DIR.mkdirs()) {
              throw new IOException("Could not create temporary directory");
            }
          }
          TEMP_DIR.deleteOnExit();
          final MiniAccumuloConfigImpl config =
              new MiniAccumuloConfigImpl(TEMP_DIR, DEFAULT_MINI_ACCUMULO_PASSWORD);
          config.setZooKeeperPort(Integer.parseInt(zookeeper.split(":")[1]));
          config.setNumTservers(NUM_TABLET_SERVERS);

          miniAccumulo =
              MiniAccumuloClusterFactory.newAccumuloCluster(
                  config,
                  AccumuloStoreTestEnvironment.class);

          startMiniAccumulo(config);
          accumuloUser = "root";
          accumuloInstance = miniAccumulo.getInstanceName();
          accumuloPassword = DEFAULT_MINI_ACCUMULO_PASSWORD;
        } catch (IOException | InterruptedException e) {
          LOGGER.warn("Unable to start mini accumulo instance", e);
          LOGGER.info(
              "Check '" + TEMP_DIR.getAbsolutePath() + File.separator + "logs' for more info");
          if (SystemUtils.IS_OS_WINDOWS) {
            LOGGER.warn(
                "For windows, make sure that Cygwin is installed and set a CYGPATH environment variable to %CYGWIN_HOME%/bin/cygpath to successfully run a mini accumulo cluster");
          }
          Assert.fail("Unable to start mini accumulo instance: '" + e.getLocalizedMessage() + "'");
        }
      }
    }
  }

  private void startMiniAccumulo(final MiniAccumuloConfigImpl config)
      throws IOException, InterruptedException {

    final LinkedList<String> jvmArgs = new LinkedList<>();
    jvmArgs.add("-XX:CompressedClassSpaceSize=512m");
    jvmArgs.add("-XX:MaxMetaspaceSize=512m");
    jvmArgs.add("-Xmx512m");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        tearDown();
      }
    });
    Configuration coreSite = new Configuration(false);
    final Map<String, String> siteConfig = config.getSiteConfig();
    siteConfig.put(Property.INSTANCE_ZK_HOST.getKey(), zookeeper);
    config.setSiteConfig(siteConfig);

    if (KerberosTestEnvironment.useKerberos()) {
      KerberosTestEnvironment.getInstance().configureMiniAccumulo(config, coreSite);
      File siteFile = new File(config.getConfDir(), "accumulo-site.xml");
      writeConfig(siteFile, config.getSiteConfig().entrySet());
      // Write out any configuration items to a file so HDFS will pick them up automatically (from
      // the classpath)
      if (coreSite.size() > 0) {
        File csFile = new File(config.getConfDir(), "core-site.xml");
        TestUtils.writeConfigToFile(csFile, coreSite);
      }
    }
    final LinkedList<String> args = new LinkedList<>();
    args.add("--instance-name");
    args.add(config.getInstanceName());
    if (!KerberosTestEnvironment.useKerberos()) {
      args.add("--password");
      args.add(config.getRootPassword());
    } else {
      args.add("--user");
      args.add(KerberosTestEnvironment.getInstance().getRootUser().getPrincipal());
    }
    final Process initProcess =
        miniAccumulo.exec(Initialize.class, jvmArgs, args.toArray(new String[0]));

    cleanup.add(initProcess);

    final int ret = initProcess.waitFor();
    if (ret != 0) {
      throw new RuntimeException(
          "Initialize process returned "
              + ret
              + ". Check the logs in "
              + config.getLogDir()
              + " for errors.");
    }

    LOGGER.info(
        "Starting MAC against instance "
            + config.getInstanceName()
            + " and zookeeper(s)  "
            + config.getZooKeepers());

    for (int i = 0; i < config.getNumTservers(); i++) {
      cleanup.add(miniAccumulo.exec(TabletServer.class, jvmArgs));
    }

    cleanup.add(miniAccumulo.exec(Master.class, jvmArgs));
    cleanup.add(miniAccumulo.exec(SimpleGarbageCollector.class, jvmArgs));
  }

  @SuppressFBWarnings("DM_DEFAULT_ENCODING")
  private void writeConfig(File file, Iterable<Map.Entry<String, String>> settings)
      throws IOException {
    try (FileWriter fileWriter = new FileWriter(file)) {
      fileWriter.append("<configuration>\n");

      for (Map.Entry<String, String> entry : settings) {
        String value =
            entry.getValue().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        fileWriter.append(
            "<property><name>"
                + entry.getKey()
                + "</name><value>"
                + value
                + "</value></property>\n");
      }
      fileWriter.append("</configuration>\n");
    }
  }

  @Override
  public void tearDown() {
    zookeeper = null;
    accumuloInstance = null;
    accumuloUser = null;
    accumuloPassword = null;
    if (miniAccumulo != null) {
      try {

        for (final Process p : cleanup) {
          p.destroy();
          p.waitFor();
        }

        for (final Process p : cleanup) {
          p.destroy();
          p.waitFor();
        }

        miniAccumulo = null;

      } catch (final InterruptedException e) {
        LOGGER.warn("Unable to stop mini accumulo instance", e);
      }
    }
    if (!KEEP_LOGS && (TEMP_DIR != null)) {
      try {
        // sleep because mini accumulo processes still have a
        // hold on the log files and there is no hook to get
        // notified when it is completely stopped

        Thread.sleep(2000);
        FileUtils.deleteDirectory(TEMP_DIR);
      } catch (final IOException | InterruptedException e) {
        LOGGER.warn("Unable to delete mini Accumulo temporary directory", e);
      }
    }
  }

  @Override
  protected void initOptions(final StoreFactoryOptions options) {
    final AccumuloRequiredOptions accumuloOpts = (AccumuloRequiredOptions) options;
    if (KerberosTestEnvironment.useKerberos()) {
      ClusterUser rootUser = KerberosTestEnvironment.getInstance().getRootUser();
      accumuloOpts.setUser(rootUser.getPrincipal());
      accumuloOpts.setKeytab(rootUser.getKeytab().getAbsolutePath());
      accumuloOpts.setUseSasl(true);
    } else {
      accumuloOpts.setUser(accumuloUser);
      accumuloOpts.setPassword(accumuloPassword);
    }
    accumuloOpts.setInstance(accumuloInstance);
    accumuloOpts.setZookeeper(zookeeper);
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.ACCUMULO;
  }

  public String getZookeeper() {
    return zookeeper;
  }

  public String getAccumuloInstance() {
    return accumuloInstance;
  }

  public String getAccumuloUser() {
    return accumuloUser;
  }

  public String getAccumuloPassword() {
    return accumuloPassword;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    if (KerberosTestEnvironment.useKerberos()) {
      return new TestEnvironment[] {
          KerberosTestEnvironment.getInstance(),
          ZookeeperTestEnvironment.getInstance()};
    }
    return new TestEnvironment[] {ZookeeperTestEnvironment.getInstance()};
  }
}
