/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.type.TypeSection;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.ingest.AbstractLocalFileIngestDriver;
import org.locationtech.geowave.core.store.ingest.DataAdapterProvider;
import org.locationtech.geowave.core.store.ingest.IngestUtils;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.ingest.LocalPluginBase;
import org.locationtech.geowave.core.store.ingest.LocalPluginFileVisitor.PluginVisitor;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.internal.Maps;
import com.clearspring.analytics.util.Lists;

@GeowaveOperation(name = "add", parentOperation = TypeSection.class)
@Parameters(commandDescription = "Add a type with a given name to the data store")
public class AddTypeCommand extends ServiceEnabledCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTypeCommand.class);

  @Parameter(description = "<file or directory> <store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VisibilityOptions visibilityOptions = new VisibilityOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private List<Index> inputIndices = null;

  @Override
  public boolean prepare(final OperationParams params) {

    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInputOptions.getFormats());

    return true;
  }

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(
      final String fileOrDirectory,
      final String storeName,
      final String commaDelimitedIndexes) {
    parameters = new ArrayList<>();
    parameters.add(fileOrDirectory);
    parameters.add(storeName);
    parameters.add(commaDelimitedIndexes);
  }

  public VisibilityOptions getVisibilityOptions() {
    return visibilityOptions;
  }

  public void setVisibilityOptions(final VisibilityOptions visibilityOptions) {
    this.visibilityOptions = visibilityOptions;
  }

  public LocalInputCommandLineOptions getLocalInputOptions() {
    return localInputOptions;
  }

  public void setLocalInputOptions(final LocalInputCommandLineOptions localInputOptions) {
    this.localInputOptions = localInputOptions;
  }

  public IngestFormatPluginOptions getPluginFormats() {
    return pluginFormats;
  }

  public void setPluginFormats(final IngestFormatPluginOptions pluginFormats) {
    this.pluginFormats = pluginFormats;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public List<Index> getInputIndices() {
    return inputIndices;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 3) {
      throw new ParameterException(
          "Requires arguments: <file or directory> <storename> <comma delimited index list>");
    }

    final String inputPath = parameters.get(0);
    final String inputStoreName = parameters.get(1);
    final String indexList = parameters.get(2);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

    final IndexStore indexStore = inputStoreOptions.createIndexStore();
    inputIndices = DataStoreUtils.loadIndices(indexStore, indexList);


    try {
      final List<DataTypeAdapter<?>> adapters = getAllDataAdapters(inputPath, configFile);
      if (adapters.size() == 0) {
        throw new ParameterException("No types could be found with the given options.");
      }
      final DataStore dataStore = inputStoreOptions.createDataStore();
      final Index[] indices = inputIndices.toArray(new Index[inputIndices.size()]);
      adapters.forEach(adapter -> {
        dataStore.addType(
            adapter,
            visibilityOptions.getConfiguredVisibilityHandler(),
            Lists.newArrayList(),
            indices);
        params.getConsole().println("Added type: " + adapter.getTypeName());
      });
    } catch (IOException e) {
      throw new RuntimeException("Failed to get data types from specified directory.", e);
    }

    return null;
  }

  public List<DataTypeAdapter<?>> getAllDataAdapters(final String inputPath, final File configFile)
      throws IOException {
    final Map<String, LocalFileIngestPlugin<?>> ingestPlugins =
        pluginFormats.createLocalIngestPlugins();
    final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<>();
    final Map<String, DataTypeAdapter<?>> adapters = Maps.newHashMap();
    for (final Entry<String, LocalFileIngestPlugin<?>> pluginEntry : ingestPlugins.entrySet()) {

      if (!isSupported(pluginEntry.getKey(), pluginEntry.getValue())) {
        continue;
      }

      localFileIngestPlugins.put(pluginEntry.getKey(), pluginEntry.getValue());

      Arrays.stream(pluginEntry.getValue().getDataAdapters()).forEach(adapter -> {
        adapters.put(adapter.getTypeName(), adapter);
      });
    }

    Properties configProperties = null;
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

    for (final LocalPluginBase localPlugin : localFileIngestPlugins.values()) {
      localPlugin.init(path.toUri().toURL());
    }

    final DataAdapterFileVisitor fileURLs =
        new DataAdapterFileVisitor(
            localFileIngestPlugins,
            localInputOptions.getExtensions(),
            adapters);
    Files.walkFileTree(path, fileURLs);

    return Lists.newArrayList(adapters.values());
  }

  /**
   * This class is used by the local file driver to recurse a directory of files and find all
   * DataAdapters that would be created by the ingest.
   */
  public static class DataAdapterFileVisitor implements FileVisitor<Path> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAdapterFileVisitor.class);

    private final List<PluginVisitor<LocalFileIngestPlugin<?>>> pluginVisitors;
    private final Map<String, DataTypeAdapter<?>> adapters;

    public DataAdapterFileVisitor(
        final Map<String, LocalFileIngestPlugin<?>> localPlugins,
        final String[] userExtensions,
        final Map<String, DataTypeAdapter<?>> adapters) {
      pluginVisitors = new ArrayList<>(localPlugins.size());
      for (final Entry<String, LocalFileIngestPlugin<?>> localPluginBase : localPlugins.entrySet()) {
        pluginVisitors.add(
            new PluginVisitor<>(
                localPluginBase.getValue(),
                localPluginBase.getKey(),
                userExtensions));
      }
      this.adapters = adapters;
    }

    @Override
    public FileVisitResult postVisitDirectory(final Path path, final IOException e)
        throws IOException {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(final Path path, final BasicFileAttributes bfa)
        throws IOException {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(final Path path, final BasicFileAttributes bfa)
        throws IOException {
      final URL file = path.toUri().toURL();
      for (final PluginVisitor<LocalFileIngestPlugin<?>> visitor : pluginVisitors) {
        if (visitor.supportsFile(file)) {
          Arrays.stream(visitor.getLocalPluginBase().getDataAdapters(file)).forEach(adapter -> {
            adapters.put(adapter.getTypeName(), adapter);
          });
        }
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(final Path path, final IOException bfa)
        throws IOException {
      LOGGER.error("Cannot visit path: " + path);
      return FileVisitResult.CONTINUE;
    }
  }

  private boolean isSupported(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider) {
    return AbstractLocalFileIngestDriver.checkIndexesAgainstProvider(
        providerName,
        adapterProvider,
        inputIndices);
  }
}
