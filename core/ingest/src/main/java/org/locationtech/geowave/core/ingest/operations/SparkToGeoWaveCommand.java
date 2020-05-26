/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.ingest.spark.SparkCommandLineOptions;
import org.locationtech.geowave.core.ingest.spark.SparkIngestDriver;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "sparkToGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Ingest supported files that already exist in HDFS or S3")
public class SparkToGeoWaveCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<input directory> <store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private VisibilityOptions ingestOptions = new VisibilityOptions();

  @ParametersDelegate
  private SparkCommandLineOptions sparkOptions = new SparkCommandLineOptions();

  @ParametersDelegate
  private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

  @Override
  public boolean prepare(final OperationParams params) {

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
    if (parameters.size() != 3) {
      throw new ParameterException(
          "Requires arguments: <input directory> <store name> <comma delimited index list>");
    }

    computeResults(params);
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(
      final String inputPath,
      final String storeName,
      final String commaSeparatedIndexes) {
    parameters = new ArrayList<>();
    parameters.add(inputPath);
    parameters.add(storeName);
    parameters.add(commaSeparatedIndexes);
  }

  public VisibilityOptions getIngestOptions() {
    return ingestOptions;
  }

  public void setIngestOptions(final VisibilityOptions ingestOptions) {
    this.ingestOptions = ingestOptions;
  }

  public SparkCommandLineOptions getSparkOptions() {
    return sparkOptions;
  }

  public void setSparkOptions(final SparkCommandLineOptions sparkOptions) {
    this.sparkOptions = sparkOptions;
  }

  public LocalInputCommandLineOptions getLocalInputOptions() {
    return localInputOptions;
  }

  public void setLocalInputOptions(final LocalInputCommandLineOptions localInputOptions) {
    this.localInputOptions = localInputOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 3) {
      throw new ParameterException(
          "Requires arguments: <file or directory> <store name> <comma delimited index list>");
    }

    final String inputPath = parameters.get(0);
    final String inputStoreName = parameters.get(1);
    final String indexList = parameters.get(2);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    // Driver
    final SparkIngestDriver driver = new SparkIngestDriver();

    // Execute
    if (!driver.runOperation(
        configFile,
        localInputOptions,
        inputStoreName,
        indexList,
        ingestOptions,
        sparkOptions,
        inputPath,
        params.getConsole())) {
      throw new RuntimeException("Ingest failed to execute");
    }

    return null;
  }
}
