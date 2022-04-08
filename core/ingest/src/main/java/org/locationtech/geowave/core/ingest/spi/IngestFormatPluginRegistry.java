/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPluginRegistrySpi;

public class IngestFormatPluginRegistry implements LocalFileIngestPluginRegistrySpi {

  private static Map<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderRegistry = null;

  public IngestFormatPluginRegistry() {}

  @SuppressWarnings("rawtypes")
  private static void initPluginProviderRegistry() {
    pluginProviderRegistry = new HashMap<>();
    final Iterator<IngestFormatPluginProviderSpi> pluginProviders =
        new SPIServiceRegistry(IngestFormatPluginRegistry.class).load(
            IngestFormatPluginProviderSpi.class);
    while (pluginProviders.hasNext()) {
      final IngestFormatPluginProviderSpi pluginProvider = pluginProviders.next();
      pluginProviderRegistry.put(
          ConfigUtils.cleanOptionName(pluginProvider.getIngestFormatName()),
          pluginProvider);
    }
  }

  public static Map<String, IngestFormatPluginProviderSpi<?, ?>> getPluginProviderRegistry() {
    if (pluginProviderRegistry == null) {
      initPluginProviderRegistry();
    }
    return pluginProviderRegistry;
  }

  @Override
  public Map<String, LocalFileIngestPlugin<?>> getDefaultLocalIngestPlugins() {
    return getPluginProviderRegistry().entrySet().stream().collect(
        Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().createLocalFileIngestPlugin(e.getValue().createOptionsInstances())));
  }
}
