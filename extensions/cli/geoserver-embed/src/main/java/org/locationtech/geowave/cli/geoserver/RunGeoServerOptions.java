/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class RunGeoServerOptions {
  private static final String DEFAULT_GEOSERVER_DIR =
      "lib/services/third-party/embedded-geoserver/geoserver";

  @Parameter(
      names = {"--port", "-p"},
      description = "Select the port for GeoServer to listen on (default is port 8080)")
  private Integer port = 8080;

  @Parameter(
      names = {"--host"},
      description = "Select the host name or IP address for GeoServer to listen on (default is 127.0.0.1, which only this machine can reach)")
  private String host = "127.0.0.1";

  @Parameter(
      names = {"--directory", "-d"},
      description = "The unpacked GeoServer WAR to run. Default is lib/services/third-party/embedded-geoserver/geoserver under ~/geowave, or under the geowave.home system property if it is set.")
  private String directory = null;

  protected static final int ACCEPT_QUEUE_SIZE = 100;
  protected static final int MAX_IDLE_TIME = (int) TimeUnit.HOURS.toMillis(1);

  public void setPort(final int port) {
    this.port = port;
  }

  public Server getServer() throws Exception {
    // Prevent "Unauthorized class found" error
    System.setProperty("GEOSERVER_XSTREAM_WHITELIST", "org.geoserver.wfs.**;org.geoserver.wms.**");

    final Server jettyServer = new Server();
    final ServerConnector conn = new ServerConnector(jettyServer);
    conn.setHost(host);
    conn.setPort(port);
    conn.setAcceptQueueSize(ACCEPT_QUEUE_SIZE);
    conn.setIdleTimeout(MAX_IDLE_TIME);
    jettyServer.setConnectors(new Connector[] {conn});

    if (directory == null) {
      directory =
          Paths.get(
              System.getProperty("geowave.home", DataStoreUtils.DEFAULT_GEOWAVE_DIRECTORY),
              DEFAULT_GEOSERVER_DIR).toString();
    }
    // Jetty starts an empty context without complaint when the directory is not a webapp
    if (!Paths.get(directory, "WEB-INF", "web.xml").toFile().isFile()) {
      throw new ParameterException(
          "No GeoServer web application found in '"
              + directory
              + "'. Unpack the GeoServer WAR into that directory, or point --directory at one.");
    }
    final GeoServerWebAppContext geoserver = new GeoServerWebAppContext(Paths.get(directory));
    // fail rather than keep serving 503s when GeoServer does not start
    geoserver.setThrowUnavailableOnStartupException(true);
    jettyServer.setHandler(geoserver);
    return jettyServer;
  }
}
