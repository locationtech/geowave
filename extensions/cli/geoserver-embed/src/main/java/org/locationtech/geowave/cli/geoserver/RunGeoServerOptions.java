/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;

public class RunGeoServerOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunGeoServerOptions.class);
  private static final String DEFAULT_GEOSERVER_DIR =
      "lib/services/third-party/embedded-geoserver/geoserver";

  private static final String[] PARENT_CLASSLOADER_LIBRARIES =
      new String[] {"hbase", "hadoop", "protobuf", "guava", "restlet", "spring"};
  @Parameter(
      names = {"--port", "-p"},
      description = "Select the port for GeoServer to listen on (default is port 8080)")
  private Integer port = 8080;

  @Parameter(
      names = {"--directory", "-d"},
      description = "The directory to use for geoserver. Default is the GeoServer in the installation directory.")
  private String directory = null;

  protected static final int ACCEPT_QUEUE_SIZE = 100;
  protected static final int MAX_IDLE_TIME = (int) TimeUnit.HOURS.toMillis(1);
  protected static final int SO_LINGER_TIME = -1;
  protected static final int MAX_FORM_CONTENT_SIZE = 1024 * 1024 * 2;
  protected static final String GEOSERVER_CONTEXT_PATH = "/geoserver";

  public Server getServer() throws Exception {

    Server jettyServer;
    // Prevent "Unauthorized class found" error
    System.setProperty("GEOSERVER_XSTREAM_WHITELIST", "org.geoserver.wfs.**;org.geoserver.wms.**");

    // delete old workspace configuration if it's still there
    jettyServer = new Server();

    final ServerConnector conn = new ServerConnector(jettyServer);
    conn.setPort(port);
    conn.setAcceptQueueSize(ACCEPT_QUEUE_SIZE);
    conn.setIdleTimeout(MAX_IDLE_TIME);
    conn.setSoLingerTime(SO_LINGER_TIME);
    jettyServer.setConnectors(new Connector[] {conn});

    final WebAppContext gsWebapp = new WebAppContext();
    gsWebapp.setContextPath(GEOSERVER_CONTEXT_PATH);
    if (directory == null) {
      directory =
          Paths.get(System.getProperty("geowave.home", "."), DEFAULT_GEOSERVER_DIR).toString();
    }
    gsWebapp.setResourceBase(directory);

    final WebAppClassLoader classLoader =
        AccessController.doPrivileged(new PrivilegedAction<WebAppClassLoader>() {
          @Override
          public WebAppClassLoader run() {
            try {
              return new WebAppClassLoader(gsWebapp);
            } catch (final IOException e) {
              LOGGER.error("Unable to create new classloader", e);
              return null;
            }
          }
        });
    if (classLoader == null) {
      throw new IOException("Unable to create classloader");
    }
    final String classpath = System.getProperty("java.class.path").replace(":", ";");
    final String[] individualEntries = classpath.split(";");
    final StringBuffer str = new StringBuffer();
    for (final String e : individualEntries) {
      // HBase has certain static initializers that use reflection
      // to get annotated values

      // because Class instances are not equal if they are loaded
      // by different class loaders this HBase initialization
      // fails

      // furthermore HBase's runtime dependencies need to
      // be loaded by the same classloader, the webapp's parent
      // class loader

      // but geowave hbase datastore implementation must be loaded
      // by the same classloader as geotools or the SPI loader
      // won't work

      boolean addLibraryToWebappContext = true;
      if (!e.contains("geowave")) {
        for (final String parentLoaderLibrary : PARENT_CLASSLOADER_LIBRARIES) {
          if (e.contains(parentLoaderLibrary)) {
            addLibraryToWebappContext = false;
            break;
          }
        }
      }
      if (addLibraryToWebappContext) {
        str.append(e).append(";");
      }
    }
    classLoader.addClassPath(str.toString());
    gsWebapp.setClassLoader(classLoader);
    // this has to be false for geoserver to load the correct guava
    // classes (until hadoop updates guava support to a later
    // version, slated for hadoop 3.x)
    gsWebapp.setParentLoaderPriority(false);
    jettyServer.setHandler(new ContextHandlerCollection(gsWebapp));
    // // this allows to send large SLD's from the styles form
    gsWebapp.getServletContext().getContextHandler().setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
    return jettyServer;
  }
}
