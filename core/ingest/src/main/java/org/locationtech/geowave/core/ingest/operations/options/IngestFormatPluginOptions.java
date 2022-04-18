/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations.options;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.api.DefaultPluginOptions;
import org.locationtech.geowave.core.cli.api.PluginOptions;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginRegistry;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

/**
 * This convenience class has methods for loading a list of plugins based on command line options
 * set by the user.
 */
public class IngestFormatPluginOptions extends DefaultPluginOptions implements PluginOptions {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestFormatPluginOptions.class);

  private String formats;

  private Map<String, IngestFormatPluginProviderSpi<?, ?>> plugins = new HashMap<>();

  @ParametersDelegate
  private HashMap<String, IngestFormatOptions> options = new HashMap<>();

  @Override
  public void selectPlugin(final String qualifier) {
    // This is specified as so: format1,format2,...
    formats = qualifier;
    if ((qualifier != null) && (qualifier.length() > 0)) {
      for (final String name : qualifier.split(",")) {
        addFormat(name.trim());
      }
    } else {
      // Add all
      for (final String formatName : IngestFormatPluginRegistry.getPluginProviderRegistry().keySet()) {
        addFormat(formatName);
      }
    }
  }

  private void addFormat(final String formatName) {

    final IngestFormatPluginProviderSpi<?, ?> formatPlugin =
        IngestFormatPluginRegistry.getPluginProviderRegistry().get(formatName);

    if (formatPlugin == null) {
      throw new ParameterException("Unknown format type specified: " + formatName);
    }

    plugins.put(formatName, formatPlugin);

    IngestFormatOptions optionObject = formatPlugin.createOptionsInstances();

    if (optionObject == null) {
      optionObject = new IngestFormatOptions() {};
    }

    options.put(formatName, optionObject);
  }

  @Override
  public String getType() {
    return formats;
  }

  public Map<String, LocalFileIngestPlugin<?>> createLocalIngestPlugins() {
    final Map<String, LocalFileIngestPlugin<?>> ingestPlugins = new HashMap<>();
    for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
      final IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
      final IngestFormatOptions formatOptions = options.get(entry.getKey());
      LocalFileIngestPlugin<?> plugin = null;
      try {
        plugin = formatPlugin.createLocalFileIngestPlugin(formatOptions);
        if (plugin == null) {
          throw new UnsupportedOperationException();
        }
      } catch (final UnsupportedOperationException e) {
        LOGGER.warn(
            "Plugin provider for ingest type '"
                + formatPlugin.getIngestFormatName()
                + "' does not support local file ingest",
            e);
        continue;
      }
      ingestPlugins.put(formatPlugin.getIngestFormatName(), plugin);
    }
    return ingestPlugins;
  }

  public Map<String, IngestFromHdfsPlugin<?, ?>> createHdfsIngestPlugins() {
    final Map<String, IngestFromHdfsPlugin<?, ?>> ingestPlugins = new HashMap<>();
    for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
      final IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
      final IngestFormatOptions formatOptions = options.get(entry.getKey());
      IngestFromHdfsPlugin<?, ?> plugin = null;
      try {
        plugin = formatPlugin.createIngestFromHdfsPlugin(formatOptions);
        if (plugin == null) {
          throw new UnsupportedOperationException();
        }
      } catch (final UnsupportedOperationException e) {
        LOGGER.warn(
            "Plugin provider for ingest type '"
                + formatPlugin.getIngestFormatName()
                + "' does not support hdfs ingest",
            e);
        continue;
      }
      ingestPlugins.put(formatPlugin.getIngestFormatName(), plugin);
    }
    return ingestPlugins;
  }

  public Map<String, GeoWaveAvroFormatPlugin<?, ?>> createAvroPlugins() {
    final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins = new HashMap<>();
    for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
      final IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
      final IngestFormatOptions formatOptions = options.get(entry.getKey());
      GeoWaveAvroFormatPlugin<?, ?> plugin = null;
      try {
        plugin = formatPlugin.createAvroFormatPlugin(formatOptions);
        if (plugin == null) {
          throw new UnsupportedOperationException();
        }
      } catch (final UnsupportedOperationException e) {
        LOGGER.warn(
            "Plugin provider for ingest type '"
                + formatPlugin.getIngestFormatName()
                + "' does not support avro ingest",
            e);
        continue;
      }
      ingestPlugins.put(formatPlugin.getIngestFormatName(), plugin);
    }
    return ingestPlugins;
  }

  public Map<String, IngestFormatPluginProviderSpi<?, ?>> getPlugins() {
    return plugins;
  }

  public void setPlugins(final Map<String, IngestFormatPluginProviderSpi<?, ?>> plugins) {
    this.plugins = plugins;
  }

  public Map<String, IngestFormatOptions> getOptions() {
    return options;
  }

  public void setOptions(final HashMap<String, IngestFormatOptions> options) {
    this.options = options;
  }
}
