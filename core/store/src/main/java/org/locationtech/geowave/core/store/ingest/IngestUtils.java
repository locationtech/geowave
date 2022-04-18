/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

public class IngestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);

  private static List<IngestUrlHandlerSpi> urlHandlerList = null;
  private static Map<String, LocalFileIngestPlugin<?>> localIngestPlugins = null;

  public static boolean checkIndexesAgainstProvider(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider,
      final List<Index> indices) {
    boolean valid = true;
    for (final Index index : indices) {
      if (!isCompatible(adapterProvider, index)) {
        // HP Fortify "Log Forging" false positive
        // What Fortify considers "user input" comes only
        // from users with OS-level access anyway
        LOGGER.warn(
            "Local file ingest plugin for ingest type '"
                + providerName
                + "' is not supported by index '"
                + index.getName()
                + "'");
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Determine whether an index is compatible with the visitor
   *
   * @param index an index that an ingest type supports
   * @return whether the adapter is compatible with the common index model
   */
  public static boolean isCompatible(
      final DataAdapterProvider<?> adapterProvider,
      final Index index) {
    final String[] supportedTypes = adapterProvider.getSupportedIndexTypes();
    if ((supportedTypes == null) || (supportedTypes.length == 0)) {
      return false;
    }
    final NumericDimensionField<?>[] requiredDimensions = index.getIndexModel().getDimensions();
    for (final NumericDimensionField<?> requiredDimension : requiredDimensions) {
      boolean fieldFound = false;
      for (final String supportedType : supportedTypes) {
        if (requiredDimension.getFieldName().equals(supportedType)) {
          fieldFound = true;
          break;
        }
      }
      if (!fieldFound) {
        return false;
      }
    }
    return true;
  }

  public static synchronized Path handleIngestUrl(
      final String ingestUrl,
      final Properties configProperties) throws IOException {
    if (urlHandlerList == null) {
      final Iterator<IngestUrlHandlerSpi> handlers =
          new SPIServiceRegistry(IngestUrlHandlerSpi.class).load(IngestUrlHandlerSpi.class);
      urlHandlerList = Lists.newArrayList(handlers);
    }
    for (final IngestUrlHandlerSpi h : urlHandlerList) {
      final Path path = h.handlePath(ingestUrl, configProperties);
      if (path != null) {
        return path;
      }
    }
    return null;
  }

  public static synchronized Map<String, LocalFileIngestPlugin<?>> getDefaultLocalIngestPlugins() {
    if (localIngestPlugins == null) {
      final Iterator<LocalFileIngestPluginRegistrySpi> registries =
          new SPIServiceRegistry(LocalFileIngestPluginRegistrySpi.class).load(
              LocalFileIngestPluginRegistrySpi.class);
      localIngestPlugins = new HashMap<>();
      while (registries.hasNext()) {
        localIngestPlugins.putAll(registries.next().getDefaultLocalIngestPlugins());
      }
    }
    return localIngestPlugins;
  }
}
