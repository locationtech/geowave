/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be sub-classed to handle recursing over a local directory structure and passing
 * along the plugin specific handling of any supported file for a discovered plugin.
 *
 * @param <P> The type of the plugin this driver supports.
 * @param <R> The type for intermediate data that can be used throughout the life of the process and
 *        is passed along for each call to process a file.
 */
public abstract class AbstractLocalFileDriver<P extends LocalPluginBase, R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLocalFileDriver.class);
  protected LocalInputCommandLineOptions localInput;
  protected Properties configProperties;

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
  protected static boolean isCompatible(
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

  public AbstractLocalFileDriver() {}

  public AbstractLocalFileDriver(final LocalInputCommandLineOptions input) {
    localInput = input;
  }

  protected void processInput(
      final String inputPath,
      final File configFile,
      final Map<String, P> localPlugins,
      final R runData) throws IOException {
    if (inputPath == null) {
      LOGGER.error("Unable to ingest data, base directory or file input not specified");
      return;
    }

    if ((configFile != null) && configFile.exists()) {
      configProperties = ConfigOptions.loadProperties(configFile);
    }
    Path path = IngestUtils.handleIngestUrl(inputPath, configProperties);
    if (path == null) {
      final File f = new File(inputPath);
      if (!f.exists()) {
        LOGGER.error("Input file '" + f.getAbsolutePath() + "' does not exist");
        throw new IllegalArgumentException(inputPath + " does not exist");
      }
      path = Paths.get(inputPath);
    }

    for (final LocalPluginBase localPlugin : localPlugins.values()) {
      localPlugin.init(path.toUri().toURL());
    }

    Files.walkFileTree(
        path,
        new LocalPluginFileVisitor<>(localPlugins, this, runData, getExtensions()));
  }

  protected String[] getExtensions() {
    return localInput.getExtensions();
  }

  protected abstract void processFile(final URL file, String typeName, P plugin, R runData)
      throws IOException;
}
