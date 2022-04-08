/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.util.ToolRunner;
import org.locationtech.geowave.cli.osm.mapreduce.Convert.OSMConversionRunner;
import org.locationtech.geowave.cli.osm.mapreduce.Ingest.OSMRunner;
import org.locationtech.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import org.locationtech.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "ingest", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Ingest and convert OSM data from HDFS to GeoWave")
public class IngestOSMToGeoWaveCommand extends DefaultOperation implements Command {

  @Parameter(description = "<path to base directory to read from> <store name>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private OSMIngestCommandArgs ingestOptions = new OSMIngestCommandArgs();

  private DataStorePluginOptions inputStoreOptions = null;

  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException(
          "Requires arguments: <path to base directory to read from> <store name>");
    }

    for (final String string : computeResults(params)) {
      params.getConsole().println(string);
    }
  }

  private List<String> ingestData() throws Exception {

    final OSMRunner runner = new OSMRunner(ingestOptions, inputStoreOptions);

    final int res = ToolRunner.run(runner, new String[] {});
    if (res != 0) {
      throw new RuntimeException("OSMRunner failed: " + res);
    }

    final List<String> output = new ArrayList<>();
    output.add("finished ingest");
    output.add("**************************************************");
    return output;
  }

  private List<String> convertData() throws Exception {

    FeatureDefinitionSet.initialize(new OSMIngestCommandArgs().getMappingContents());

    final OSMConversionRunner runner = new OSMConversionRunner(ingestOptions, inputStoreOptions);

    final int res = ToolRunner.run(runner, new String[] {});
    if (res != 0) {
      throw new RuntimeException("OSMConversionRunner failed: " + res);
    }

    final List<String> output = new ArrayList<>();
    output.add("finished conversion");
    output.add("**************************************************");
    output.add("**************************************************");
    output.add("**************************************************");
    return output;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String hdfsPath, final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(hdfsPath);
    parameters.add(storeName);
  }

  public OSMIngestCommandArgs getIngestOptions() {
    return ingestOptions;
  }

  public void setIngestOptions(final OSMIngestCommandArgs ingestOptions) {
    this.ingestOptions = ingestOptions;
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  public List<String> computeResults(final OperationParams params) throws Exception {
    final String basePath = parameters.get(0);
    final String inputStoreName = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProperties = ConfigOptions.loadProperties(configFile);
    final String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

    inputStoreOptions = CLIUtils.loadStore(inputStoreName, configFile, params.getConsole());

    // Copy over options from main parameter to ingest options
    ingestOptions.setHdfsBasePath(basePath);
    ingestOptions.setNameNode(hdfsHostPort);

    if (inputStoreOptions.getGeoWaveNamespace() == null) {
      inputStoreOptions.getFactoryOptions().setGeoWaveNamespace("osmnamespace");
    }

    if (ingestOptions.getVisibilityOptions().getGlobalVisibility() == null) {
      ingestOptions.getVisibilityOptions().setGlobalVisibility("public");
    }

    // This is needed by a method in OSMIngsetCommandArgs.
    ingestOptions.setOsmNamespace(inputStoreOptions.getGeoWaveNamespace());

    final List<String> outputs = new ArrayList<>();

    // Ingest the data.
    outputs.addAll(ingestData());

    // Convert the data
    outputs.addAll(convertData());

    return outputs;
  }
}
