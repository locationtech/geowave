/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.locationtech.geowave.cli.geoserver.GeoServerWebAppContext;
import org.locationtech.geowave.service.rest.GeoWaveRestApplication;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestEnvironment;
import org.locationtech.geowave.test.kafka.KafkaTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sun.net.httpserver.HttpServer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ServicesTestEnvironment implements TestEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServicesTestEnvironment.class);

  private static ServicesTestEnvironment singletonInstance = null;

  public static synchronized ServicesTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ServicesTestEnvironment();
    }
    return singletonInstance;
  }

  protected static final int JETTY_PORT = 9011;
  protected static final String JETTY_BASE_URL = "http://localhost:" + JETTY_PORT;
  protected static final int ACCEPT_QUEUE_SIZE = 100;
  protected static final int MAX_IDLE_TIME = (int) TimeUnit.HOURS.toMillis(1);
  protected static final String GEOSERVER_USER = "admin";
  protected static final String GEOSERVER_PASS = "geoserver";
  protected static final String TEST_WORKSPACE = "geowave_test";
  protected static final String GEOSERVER_WAR_DIR = "target/geoserver";
  protected static final String GEOSERVER_CONTEXT_PATH = GeoServerWebAppContext.CONTEXT_PATH;
  protected static final String GEOSERVER_BASE_URL = JETTY_BASE_URL + GEOSERVER_CONTEXT_PATH;
  protected static final String GEOSERVER_REST_PATH = GEOSERVER_BASE_URL + "/rest";
  protected static final int REST_PORT = 9012;
  protected static final String GEOWAVE_CONTEXT_PATH = "/restservices";
  protected static final String GEOWAVE_BASE_URL =
      "http://localhost:" + REST_PORT + GEOWAVE_CONTEXT_PATH;

  protected static final String GEOWAVE_CONFIG_FILE = "target/restservices/config.properties";
  protected static final String GEOWAVE_WORKSPACE_PATH =
      GEOSERVER_WAR_DIR + "/data/workspaces/" + TEST_WORKSPACE;
  protected static final String TEST_STYLE_NAME_NO_DIFFERENCE = "SubsamplePoints-2px";
  protected static final String TEST_STYLE_NAME_MINOR_SUBSAMPLE = "SubsamplePoints-10px";
  protected static final String TEST_STYLE_NAME_MAJOR_SUBSAMPLE = "SubsamplePoints-100px";
  protected static final String TEST_STYLE_NAME_DISTRIBUTED_RENDER = "DistributedRender";
  protected static final String TEST_STYLE_PATH = "src/test/resources/sld/";
  protected static final String TEST_GEOSERVER_LOGGING_PATH = "src/test/resources/logging.xml";
  protected static final String TEST_LOG_PROPERTIES_PATH =
      "src/test/resources/log4j-test.properties";
  protected static final String TEST_GEOSERVER_LOG_PROPERTIES_PATH =
      GEOSERVER_WAR_DIR + "/data/logs/log4j-test.properties";
  protected static final String EXISTING_GEOSERVER_LOGGING_PATH =
      GEOSERVER_WAR_DIR + "/data/logging.xml";
  protected static final String TEST_SLD_NO_DIFFERENCE_FILE =
      TEST_STYLE_PATH + TEST_STYLE_NAME_NO_DIFFERENCE + ".sld";
  protected static final String TEST_SLD_MINOR_SUBSAMPLE_FILE =
      TEST_STYLE_PATH + TEST_STYLE_NAME_MINOR_SUBSAMPLE + ".sld";
  protected static final String TEST_SLD_MAJOR_SUBSAMPLE_FILE =
      TEST_STYLE_PATH + TEST_STYLE_NAME_MAJOR_SUBSAMPLE + ".sld";
  protected static final String TEST_SLD_DISTRIBUTED_RENDER_FILE =
      TEST_STYLE_PATH + TEST_STYLE_NAME_DISTRIBUTED_RENDER + ".sld";

  private Server jettyServer;
  private HttpServer restServer;

  @SuppressFBWarnings(
      value = {"SWL_SLEEP_WITH_LOCK_HELD"},
      justification = "Jetty must be started before releasing the lock")
  @Override
  public void setup() throws Exception {
    synchronized (GeoWaveITRunner.MUTEX) {
      // Setup activities delegated to private function
      // to satisfy HP Fortify
      doSetup();
    }
  }

  private void doSetup() {
    if (jettyServer == null) {
      try {
        // Prevent "Unauthorized class found" error
        System.setProperty(
            "GEOSERVER_XSTREAM_WHITELIST",
            "org.geoserver.wfs.**;org.geoserver.wms.**");

        jettyServer = new Server();

        final ServerConnector conn = new ServerConnector(jettyServer);
        conn.setPort(JETTY_PORT);
        conn.setAcceptQueueSize(ACCEPT_QUEUE_SIZE);
        conn.setIdleTimeout(MAX_IDLE_TIME);
        jettyServer.setConnectors(new Connector[] {conn});
        FileUtils.copyFile(
            new File(TEST_GEOSERVER_LOGGING_PATH),
            new File(EXISTING_GEOSERVER_LOGGING_PATH));
        FileUtils.copyFile(
            new File(TEST_LOG_PROPERTIES_PATH),
            new File(TEST_GEOSERVER_LOG_PROPERTIES_PATH));
        final GeoServerWebAppContext geoserver =
            new GeoServerWebAppContext(Paths.get(GEOSERVER_WAR_DIR));
        jettyServer.setHandler(geoserver);

        jettyServer.start();
        while (!jettyServer.isRunning() && !jettyServer.isStarted()) {
          Thread.sleep(1000);
        }
        // Jetty reports this at WARN, which the tests' logging does not show
        if (geoserver.getUnavailableException() != null) {
          LOGGER.error("GeoServer did not start", geoserver.getUnavailableException());
        }

        final File configFile = new File(GEOWAVE_CONFIG_FILE);
        FileUtils.forceMkdirParent(configFile);
        if (configFile.exists() && !configFile.delete()) {
          LOGGER.warn("Unable to delete config file");
        }
        restServer =
            JdkHttpServerFactory.createHttpServer(
                URI.create(GEOWAVE_BASE_URL + "/"),
                new GeoWaveRestApplication().property(
                    GeoWaveRestApplication.CONFIG_FILE_PROPERTY,
                    GEOWAVE_CONFIG_FILE));
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        LOGGER.error("Could not start the Jetty server: " + e.getMessage(), e);

        if (jettyServer.isRunning()) {
          try {
            jettyServer.stop();
          } catch (final Exception e1) {
            LOGGER.error("Unable to stop the Jetty server", e1);
          }
        }
      }
    }
  }

  public void restartServices() throws Exception {
    if (jettyServer != null) {
      stopRestServices();
      jettyServer.stop();
      jettyServer = null;
      doSetup();
    }
  }

  private void stopRestServices() {
    if (restServer != null) {
      restServer.stop(0);
      restServer = null;
    }
  }

  @Override
  public void tearDown() throws Exception {
    synchronized (GeoWaveITRunner.MUTEX) {
      if (!GeoWaveITRunner.DEFER_CLEANUP.get()) {
        if (jettyServer != null) {
          try {
            stopRestServices();
            jettyServer.stop();
            jettyServer = null;
            if (!new File(GEOWAVE_CONFIG_FILE).delete()) {
              LOGGER.warn("Unable to delete config file");
            }
          } catch (final Exception e) {
            LOGGER.error("Unable to stop the Jetty server", e);
          }
        }
      }
    }
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {
        MapReduceTestEnvironment.getInstance(),
        KafkaTestEnvironment.getInstance()};
  }
}
