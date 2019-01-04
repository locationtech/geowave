/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.raster;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.DoubleConverter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterOptionProvider implements IngestFormatOptions {
  // for now, default to no merging
  private static final RasterTileMergeStrategy DEFAULT_MERGE_STRATEGY = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(RasterOptionProvider.class);
  private static Map<String, RasterMergeStrategyProviderSpi> registeredMergeStrategies = null;

  @Parameter(
      names = "--pyramid",
      description = "Build an image pyramid on ingest for quick reduced resolution query")
  private final boolean buildPyramid = false;

  @Parameter(names = "--crs", description = "A CRS override for the provided raster file")
  private final String crs = null;

  @Parameter(
      names = "--histogram",
      description = "Build a histogram of samples per band on ingest for performing band equalization")
  private final boolean buildHistogram = false;

  @Parameter(
      names = "--tileSize",
      description = "Optional parameter to set the tile size stored (default is 256)")
  private final int tileSize = RasterDataAdapter.DEFAULT_TILE_SIZE;

  @Parameter(
      names = "--coverage",
      description = "Optional parameter to set the coverage name (default is the file name)")
  private final String coverageName = null;

  @Parameter(
      names = "--nodata",
      variableArity = true,
      description = "Optional parameter to set 'no data' values, if 1 value is giving it is applied for each band, if multiple are given then the first totalNoDataValues/totalBands are applied to the first band and so on, so each band can have multiple differing 'no data' values if needed",
      converter = DoubleConverter.class)
  private final List<Double> nodata = new ArrayList<>();

  @Parameter(
      names = "--separateBands",
      description = "Optional parameter to separate each band into its own coverage name. By default the coverage name will have '_Bn' appended to it where `n` is the band's index.")
  private final boolean separateBands = false;

  @Parameter(
      names = "--mergeStrategy",
      description = "Optional parameter to choose a tile merge strategy used for mosaic.  Default behavior will be `none`.  Alternatively 'no-data' will mosaic the most recent tile over previous tiles, except where there are no data values.")
  private final String mergeStrategy = NoMergeStrategyProvider.NAME;

  public RasterOptionProvider() {}

  public boolean isBuildPyramid() {
    return buildPyramid;
  }

  public int getTileSize() {
    return tileSize;
  }

  public boolean isSeparateBands() {
    return separateBands;
  }

  public String getCrs() {
    return crs;
  }

  public String getCoverageName() {
    if ((coverageName == null) || coverageName.trim().isEmpty()) {
      return null;
    }
    return coverageName;
  }

  public boolean isBuildHistogram() {
    return buildHistogram;
  }

  public double[][] getNodata(final int numBands) {
    if (nodata.isEmpty() || (numBands <= 0)) {
      return null;
    }
    final double[][] retVal = new double[numBands][];
    final int nodataPerBand = nodata.size() / numBands;
    if (nodataPerBand <= 1) {
      for (int b = 0; b < numBands; b++) {
        retVal[b] = new double[] {nodata.get(Math.min(b, nodata.size() - 1))};
      }
    } else {
      for (int b = 0; b < retVal.length; b++) {
        retVal[b] = new double[nodataPerBand];
        for (int i = 0; i < nodataPerBand; i++) {
          retVal[b][i] = nodata.get((b * nodataPerBand) + i);
        }
      }
    }
    return retVal;
  }

  public RasterTileMergeStrategy<?> getMergeStrategy() {
    final Map<String, RasterMergeStrategyProviderSpi> internalMergeStrategies =
        getRegisteredMergeStrategies();
    if ((mergeStrategy == null) || mergeStrategy.trim().isEmpty()) {
      LOGGER.warn("Merge Strategy not found");
      return DEFAULT_MERGE_STRATEGY;
    }
    final RasterMergeStrategyProviderSpi provider = internalMergeStrategies.get(mergeStrategy);
    if (provider == null) {
      LOGGER.warn("Merge Strategy Provider not found for '" + mergeStrategy + "'");
      return DEFAULT_MERGE_STRATEGY;
    }
    return provider.getStrategy();
  }

  private synchronized Map<String, RasterMergeStrategyProviderSpi> getRegisteredMergeStrategies() {
    if (registeredMergeStrategies == null) {
      registeredMergeStrategies = new HashMap<String, RasterMergeStrategyProviderSpi>();
      final ServiceLoader<RasterMergeStrategyProviderSpi> converters =
          ServiceLoader.load(RasterMergeStrategyProviderSpi.class);
      final Iterator<RasterMergeStrategyProviderSpi> it = converters.iterator();
      while (it.hasNext()) {
        final RasterMergeStrategyProviderSpi converter = it.next();
        registeredMergeStrategies.put(converter.getName(), converter);
      }
    }
    return registeredMergeStrategies;
  }
}
