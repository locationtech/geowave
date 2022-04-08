/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.cli;

import java.io.IOException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraServer.class);

  protected static final String NODE_DIRECTORY_PREFIX = "cassandra";
  private final EmbeddedCassandraService embeddedService;

  public CassandraServer() {
    embeddedService = new EmbeddedCassandraService();
  }

  public void start() {
    try {
      embeddedService.start();
    } catch (final IOException e) {
      LOGGER.warn("Unable to start Cassandra", e);
    }
  }

  public void stop() {
    embeddedService.stop();
  }
}
