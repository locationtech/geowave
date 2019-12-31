package org.locationtech.geowave.adapter.raster.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.stats.OverviewStatistics;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
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
  private final Integer level = null;
  @Parameter(
      names = "--coverage",
      description = "The raster coverage name (required if store has multiple coverages)")
  private final String coverageName = null;

  private DataStorePluginOptions inputStoreOptions = null;


  @Override
  public void execute(final OperationParams params) throws Exception {
    run(params);
  }

  public void run(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }

    final String inputStoreName = parameters.get(0);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    // Attempt to load input store.
    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();
    final DataStore store = inputStoreLoader.createDataStore();
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
        partitions.add(new ByteArray(Arrays.append(split, level.byteValue())));
      }
    } else {
      partitions.add(new ByteArray(new byte[] {level.byteValue()}));
    }
    // delete the resolution from the overview, delete the partitions, and delete the data
    if (inputStoreOptions.getFactoryOptions().getStoreOptions().isPersistDataStatistics()) {
      final DataStatisticsStore statsStore = inputStoreLoader.createDataStatisticsStore();
      final InternalAdapterStore adapterIdStore = inputStoreLoader.createInternalAdapterStore();
      OverviewStatistics ovStats = null;
      PartitionStatistics<?> pStats = null;
      try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it =
          statsStore.getDataStatistics(
              adapterIdStore.getAdapterId(adapter.getTypeName()),
              OverviewStatistics.STATS_TYPE)) {
        while (it.hasNext()) {
          final InternalDataStatistics<?, ?, ?> next = it.next();
          if (next instanceof OverviewStatistics) {
            ovStats = (OverviewStatistics) next;
            break;
          }
        }
      }
      if (ovStats == null) {
        LOGGER.error("Unable to find overview stats for coverage " + adapter.getTypeName());
        return;
      }
      if (!ovStats.removeResolution(res)) {
        LOGGER.error("Unable to remove resolution for pyramid level " + level);
        return;
      }
      try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it =
          statsStore.getDataStatistics(
              adapterIdStore.getAdapterId(adapter.getTypeName()),
              i.getName(),
              PartitionStatistics.STATS_TYPE)) {
        while (it.hasNext()) {
          final InternalDataStatistics<?, ?, ?> next = it.next();
          if (next instanceof PartitionStatistics) {
            pStats = (PartitionStatistics) next;
            break;
          }
        }
      }
      if (pStats == null) {
        LOGGER.error(
            "Unable to find partition stats for coverage "
                + adapter.getTypeName()
                + " and index "
                + i.getName());
        return;
      }
      for (final ByteArray p : partitions) {
        if (!pStats.getPartitionKeys().remove(p)) {
          LOGGER.error(
              "Unable to remove partition " + p.getHexString() + " for pyramid level " + level);
          return;
        }
      }
      statsStore.setStatistics(ovStats);
      statsStore.setStatistics(pStats);
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
