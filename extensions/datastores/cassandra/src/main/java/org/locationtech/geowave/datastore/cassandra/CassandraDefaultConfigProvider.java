/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra;

import java.util.Properties;
import org.locationtech.geowave.core.cli.spi.DefaultConfigProviderSpi;

public class CassandraDefaultConfigProvider implements DefaultConfigProviderSpi {
  private final Properties configProperties = new Properties();

  /**
   * Create the properties for the config-properties file
   */
  private void setProperties() {
    configProperties.setProperty("store.default-cassandra.opts.gwNamespace", "default");
    configProperties.setProperty("store.default-cassandra.type", "cassandra");
    configProperties.setProperty("store.default-cassandra.opts.contactPoints", "127.0.0.1");
  }

  @Override
  public Properties getDefaultConfig() {
    setProperties();
    return configProperties;
  }
}
