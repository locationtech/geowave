/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.operations.options;

import com.beust.jcommander.Parameter;

public class RasterTileResizeCommandLineOptions {
  @Parameter(
      names = "--inputCoverageName",
      description = "The name of the input raster coverage",
      required = true)
  private String inputCoverageName;

  @Parameter(
      names = "--outputCoverageName",
      description = "The out output raster coverage name",
      required = true)
  private String outputCoverageName;

  @Parameter(names = "--minSplits", description = "The min partitions for the input data")
  private Integer minSplits;

  @Parameter(names = "--maxSplits", description = "The max partitions for the input data")
  private Integer maxSplits;

  @Parameter(names = "--outputTileSize", description = "The tile size to output", required = true)
  private Integer outputTileSize;

  @Parameter(names = "--indexName", description = "The index that the input raster is stored in")
  private String indexName;

  // Default constructor
  public RasterTileResizeCommandLineOptions() {}

  public RasterTileResizeCommandLineOptions(
      final String inputCoverageName,
      final String outputCoverageName,
      final Integer minSplits,
      final Integer maxSplits,
      final Integer outputTileSize,
      final String indexName) {
    this.inputCoverageName = inputCoverageName;
    this.outputCoverageName = outputCoverageName;
    this.minSplits = minSplits;
    this.maxSplits = maxSplits;
    this.outputTileSize = outputTileSize;
    this.indexName = indexName;
  }

  public String getInputCoverageName() {
    return inputCoverageName;
  }

  public String getOutputCoverageName() {
    return outputCoverageName;
  }

  public Integer getMinSplits() {
    return minSplits;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public Integer getOutputTileSize() {
    return outputTileSize;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setInputCoverageName(final String inputCoverageName) {
    this.inputCoverageName = inputCoverageName;
  }

  public void setOutputCoverageName(final String outputCoverageName) {
    this.outputCoverageName = outputCoverageName;
  }

  public void setMinSplits(final Integer minSplits) {
    this.minSplits = minSplits;
  }

  public void setMaxSplits(final Integer maxSplits) {
    this.maxSplits = maxSplits;
  }

  public void setOutputTileSize(final Integer outputTileSize) {
    this.outputTileSize = outputTileSize;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }
}
