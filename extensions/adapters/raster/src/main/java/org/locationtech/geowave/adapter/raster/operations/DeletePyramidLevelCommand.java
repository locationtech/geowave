/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.operations;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.stats.RasterOverviewStatistic;
import org.locationtech.geowave.adapter.raster.stats.RasterOverviewStatistic.RasterOverviewValue;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;
import org.locationtech.geowave.core.store.util.CompoundHierarchicalIndexStrategyWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "deletelevel", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Delete a pyramid level of a raster layer")
public class DeletePyramidLevelCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletePyramidLevelCommand.class);
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(names = "--level", description = "The raster pyramid level to delete", required = true)
  private Integer level = null;
  @Parameter(
      names = "--coverage",
      description = "The raster coverage name (required if store has multiple coverages)")
  private String coverageName = null;

  private DataStorePluginOptions inputStoreOptions = null;


  @Override
  public void execute(final OperationParams params) throws Exception {
    run(params);
  }

  public void setLevel(final Integer level) {
    this.level = level;
  }

  public void setCoverageName(final String coverageName) {
    this.coverageName = coverageName;
  }

  public void run(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }

    final String inputStoreName = parameters.get(0);

    // Attempt to load store.
    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    final DataStore store = inputStoreOptions.createDataStore();
    RasterDataAdapter adapter = null;

    for (final DataTypeAdapter<?> type : store.getTypes()) {
      if (isRaster(type)
          && ((coverageName == null) || coverageName.equals(adapter.getTypeName()))) {
        if (adapter != null) {
          LOGGER.error(
              "Store has multiple coverages.  Must explicitly choose one with --coverage option.");
          return;
        }
        adapter = (RasterDataAdapter) type;
      }
    }
    if (adapter == null) {
      LOGGER.error("Store has no coverages or coverage name not found.");
      return;
    }
    boolean found = false;
    Resolution res = null;
    Index i = null;
    for (final Index index : store.getIndices(adapter.getTypeName())) {
      final HierarchicalNumericIndexStrategy indexStrategy =
          CompoundHierarchicalIndexStrategyWrapper.findHierarchicalStrategy(
              index.getIndexStrategy());
      if (indexStrategy != null) {
        for (final SubStrategy s : indexStrategy.getSubStrategies()) {
          if ((s.getPrefix().length == 1) && (s.getPrefix()[0] == level)) {
            LOGGER.info("Deleting from index " + index.getName());
            final double[] tileRes = s.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
            final double[] pixelRes = new double[tileRes.length];
            for (int d = 0; d < tileRes.length; d++) {
              pixelRes[d] = tileRes[d] / adapter.getTileSize();
            }
            found = true;
            i = index;
            res = new Resolution(pixelRes);
            break;
          }
        }
      }
      if (found) {
        break;
      }

    }
    if (!found) {
      LOGGER.error("Store has no indices supporting pyramids.");
      return;
    }
    final byte[][] predefinedSplits = i.getIndexStrategy().getPredefinedSplits();
    // this should account for hash partitioning if used
    final List<ByteArray> partitions = new ArrayList<>();
    if ((predefinedSplits != null) && (predefinedSplits.length > 0)) {
      for (final byte[] split : predefinedSplits) {
        partitions.add(new ByteArray(ArrayUtils.add(split, level.byteValue())));
      }
    } else {
      partitions.add(new ByteArray(new byte[] {level.byteValue()}));
    }
    // delete the resolution from the overview, delete the partitions, and delete the data
    if (inputStoreOptions.getFactoryOptions().getStoreOptions().isPersistDataStatistics()) {
      final DataStatisticsStore statsStore = inputStoreOptions.createDataStatisticsStore();

      boolean overviewStatsFound = false;
      boolean partitionStatsFound = false;
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> it =
          statsStore.getDataTypeStatistics(adapter, RasterOverviewStatistic.STATS_TYPE, null)) {
        while (it.hasNext()) {
          final Statistic<? extends StatisticValue<?>> next = it.next();
          if ((next instanceof RasterOverviewStatistic) && (next.getBinningStrategy() == null)) {
            final RasterOverviewStatistic statistic = (RasterOverviewStatistic) next;
            final RasterOverviewValue value = statsStore.getStatisticValue(statistic);
            if (!value.removeResolution(res)) {
              LOGGER.error("Unable to remove resolution for pyramid level " + level);
              return;
            }
            statsStore.setStatisticValue(statistic, value);
            overviewStatsFound = true;
          }
        }
      }
      if (!overviewStatsFound) {
        LOGGER.error("Unable to find overview stats for coverage " + adapter.getTypeName());
        return;
      }
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> it =
          statsStore.getIndexStatistics(i, PartitionsStatistic.STATS_TYPE, null)) {
        while (it.hasNext()) {
          final Statistic<? extends StatisticValue<?>> next = it.next();
          if (next instanceof PartitionsStatistic) {
            if ((next.getBinningStrategy() != null)
                && (next.getBinningStrategy() instanceof DataTypeBinningStrategy)) {
              final PartitionsStatistic statistic = (PartitionsStatistic) next;
              final PartitionsValue value =
                  statsStore.getStatisticValue(
                      (PartitionsStatistic) next,
                      DataTypeBinningStrategy.getBin(adapter));
              for (final ByteArray p : partitions) {
                if (!value.getValue().remove(p)) {
                  LOGGER.error(
                      "Unable to remove partition "
                          + p.getHexString()
                          + " for pyramid level "
                          + level);
                  return;
                }
              }
              statsStore.setStatisticValue(
                  statistic,
                  value,
                  DataTypeBinningStrategy.getBin(adapter));
              partitionStatsFound = true;
            }
          }
        }
      }
      if (!partitionStatsFound) {
        LOGGER.error(
            "Unable to find partition stats for coverage "
                + adapter.getTypeName()
                + " and index "
                + i.getName());
        return;
      }
    }
    for (final ByteArray p : partitions) {
      store.delete(
          QueryBuilder.newBuilder().constraints(
              QueryBuilder.newBuilder().constraintsFactory().prefix(
                  p.getBytes(),
                  null)).addTypeName(adapter.getTypeName()).indexName(i.getName()).build());
    }
  }

  private static boolean isRaster(final DataTypeAdapter<?> adapter) {
    if (adapter instanceof InternalDataAdapter) {
      return isRaster(((InternalDataAdapter) adapter).getAdapter());
    }
    return adapter instanceof RasterDataAdapter;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String inputStore) {
    parameters = new ArrayList<>();
    parameters.add(inputStore);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }
}
