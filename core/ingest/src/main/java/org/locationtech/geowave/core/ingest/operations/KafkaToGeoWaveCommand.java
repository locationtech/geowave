/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.ingest.kafka.IngestFromKafkaDriver;
import org.locationtech.geowave.core.ingest.kafka.KafkaConsumerCommandLineOptions;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "kafkaToGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Subscribe to a Kafka topic and ingest into GeoWave")
public class KafkaToGeoWaveCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VisibilityOptions visibilityOptions = new VisibilityOptions();

  @ParametersDelegate
  private KafkaConsumerCommandLineOptions kafkaOptions = new KafkaConsumerCommandLineOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  // This helper is used to load the list of format SPI plugins that will be
  // used
  @ParametersDelegate
  private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

  private DataStorePluginOptions inputStoreOptions = null;

  private List<Index> inputIndices = null;

  protected IngestFromKafkaDriver driver = null;

  @Override
  public boolean prepare(final OperationParams params) {

    // TODO: localInputOptions has 'extensions' which doesn't mean
    // anything for Kafka to Geowave

    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInputOptions.getFormats());

    return true;
  }

  /**
   * Prep the driver & run the operation.
   *
   * @throws Exception
   */
  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <comma delimited index list>");
    }

    computeResults(params);
  }

  @Override
  public boolean runAsync() {
    return true;
  }

  public IngestFromKafkaDriver getDriver() {
    return driver;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String commaSeparatedIndexes) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
    parameters.add(commaSeparatedIndexes);
  }

  public VisibilityOptions getVisibilityOptions() {
    return visibilityOptions;
  }

  public void setVisibilityOptions(final VisibilityOptions visibilityOptions) {
    this.visibilityOptions = visibilityOptions;
  }

  public KafkaConsumerCommandLineOptions getKafkaOptions() {
    return kafkaOptions;
  }

  public void setKafkaOptions(final KafkaConsumerCommandLineOptions kafkaOptions) {
    this.kafkaOptions = kafkaOptions;
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
  public Void computeResults(final OperationParams params) throws Exception {
    final String inputStoreName = parameters.get(0);
    final String indexList = parameters.get(1);

    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    final IndexStore indexStore = inputStoreOptions.createIndexStore();
    inputIndices = DataStoreUtils.loadIndices(indexStore, indexList);

    // Ingest Plugins
    final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins =
        pluginFormats.createAvroPlugins();

    // Driver
    driver =
        new IngestFromKafkaDriver(
            inputStoreOptions,
            inputIndices,
            ingestPlugins,
            kafkaOptions,
            visibilityOptions.getConfiguredVisibilityHandler());

    // Execute
    if (!driver.runOperation()) {
      throw new RuntimeException("Ingest failed to execute");
    }
    return null;
  }
}
