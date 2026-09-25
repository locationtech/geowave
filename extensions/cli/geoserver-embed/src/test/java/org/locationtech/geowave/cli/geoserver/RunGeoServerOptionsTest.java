/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class RunGeoServerOptionsTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private static Server serverFor(final File directory, final String... extraArgs)
      throws Exception {
    final String[] args = new String[extraArgs.length + 2];
    args[0] = "--directory";
    args[1] = directory.getAbsolutePath();
    System.arraycopy(extraArgs, 0, args, 2, extraArgs.length);
    final RunGeoServerOptions options = new RunGeoServerOptions();
    JCommander.newBuilder().addObject(options).build().parse(args);
    return options.getServer();
  }

  private static ServerConnector connectorOf(final Server server) {
    return (ServerConnector) server.getConnectors()[0];
  }

  private File unpackedWar() throws Exception {
    final File directory = tempFolder.newFolder("geoserver");
    final File webInf = new File(directory, "WEB-INF");
    assertTrue(webInf.mkdirs());
    Files.write(
        new File(webInf, "web.xml").toPath(),
        "<web-app/>".getBytes(StandardCharsets.UTF_8));
    return directory;
  }

  @Test
  public void rejectsDirectoryWithoutWebapp() throws Exception {
    final File directory = tempFolder.newFolder("geoserver");
    try {
      serverFor(directory);
      fail("Expected a ParameterException for a directory without WEB-INF/web.xml");
    } catch (final ParameterException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(directory.getAbsolutePath()));
    }
    assertFalse(new File(directory, "data").exists());
  }

  @Test
  public void acceptsUnpackedWar() throws Exception {
    final Server server = serverFor(unpackedWar());
    assertFalse(server.isStarted());
  }

  @Test
  public void listensOnLoopbackByDefault() throws Exception {
    final ServerConnector connector = connectorOf(serverFor(unpackedWar(), "--port", "0"));
    assertEquals("127.0.0.1", connector.getHost());
    connector.open();
    try {
      final InetSocketAddress bound =
          (InetSocketAddress) ((ServerSocketChannel) connector.getTransport()).getLocalAddress();
      assertTrue(bound.toString(), bound.getAddress().isLoopbackAddress());
    } finally {
      connector.close();
    }
  }

  @Test
  public void listensOnChosenHost() throws Exception {
    assertEquals("0.0.0.0", connectorOf(serverFor(unpackedWar(), "--host", "0.0.0.0")).getHost());
  }
}
