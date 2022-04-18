/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.resize;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.adapter.GridCoverageWritable;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import org.locationtech.geowave.adapter.raster.resize.RasterTileResizeHelper;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jersey.repackaged.com.google.common.collect.Iterables;
import jersey.repackaged.com.google.common.collect.Iterators;
import scala.Tuple2;

public class RasterTileResizeSparkRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(RasterTileResizeSparkRunner.class);

  private String appName = "RasterResizeRunner";
  private String master = "yarn";
  private String host = "localhost";

  private JavaSparkContext jsc = null;
  private SparkSession session = null;
  private final DataStorePluginOptions inputStoreOptions;
  private final DataStorePluginOptions outputStoreOptions;
  protected RasterTileResizeCommandLineOptions rasterResizeOptions;

  public RasterTileResizeSparkRunner(
      final DataStorePluginOptions inputStoreOptions,
      final DataStorePluginOptions outputStoreOptions,
      final RasterTileResizeCommandLineOptions rasterResizeOptions) {
    this.inputStoreOptions = inputStoreOptions;
    this.outputStoreOptions = outputStoreOptions;
    this.rasterResizeOptions = rasterResizeOptions;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
  }

  public void setMaster(final String master) {
    this.master = master;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  private void initContext() {
    if (session == null) {
      String jar = "";
      try {
        jar =
            RasterTileResizeSparkRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        if (!FilenameUtils.isExtension(jar.toLowerCase(), "jar")) {
          jar = "";
        }
      } catch (final URISyntaxException e) {
        LOGGER.error("Unable to set jar location in spark configuration", e);
      }

      session = GeoWaveSparkConf.createSessionFromParams(appName, master, host, jar);

      jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
    }
  }

  public void run() throws IOException {
    initContext();

    // Validate inputs
    if (inputStoreOptions == null) {
      LOGGER.error("You must supply an input datastore!");
      throw new IOException("You must supply an input datastore!");
    }

    final InternalAdapterStore internalAdapterStore =
        inputStoreOptions.createInternalAdapterStore();
    final short internalAdapterId =
        internalAdapterStore.getAdapterId(rasterResizeOptions.getInputCoverageName());
    final DataTypeAdapter adapter =
        inputStoreOptions.createAdapterStore().getAdapter(internalAdapterId).getAdapter();

    if (adapter == null) {
      throw new IllegalArgumentException(
          "Adapter for coverage '"
              + rasterResizeOptions.getInputCoverageName()
              + "' does not exist in namespace '"
              + inputStoreOptions.getGeoWaveNamespace()
              + "'");
    }
    Index index = null;
    final IndexStore indexStore = inputStoreOptions.createIndexStore();
    if (rasterResizeOptions.getIndexName() != null) {
      index = indexStore.getIndex(rasterResizeOptions.getIndexName());
    }
    if (index == null) {
      try (CloseableIterator<Index> indices = indexStore.getIndices()) {
        index = indices.next();
      }
      if (index == null) {
        throw new IllegalArgumentException(
            "Index does not exist in namespace '" + inputStoreOptions.getGeoWaveNamespace() + "'");
      }
    }
    final RasterDataAdapter newAdapter =
        new RasterDataAdapter(
            (RasterDataAdapter) adapter,
            rasterResizeOptions.getOutputCoverageName(),
            rasterResizeOptions.getOutputTileSize());
    final DataStore store = outputStoreOptions.createDataStore();
    store.addType(newAdapter, index);
    final short newInternalAdapterId =
        outputStoreOptions.createInternalAdapterStore().addTypeName(newAdapter.getTypeName());
    final RDDOptions options = new RDDOptions();
    if (rasterResizeOptions.getMinSplits() != null) {
      options.setMinSplits(rasterResizeOptions.getMinSplits());
    }
    if (rasterResizeOptions.getMaxSplits() != null) {
      options.setMaxSplits(rasterResizeOptions.getMaxSplits());
    }
    final JavaPairRDD<GeoWaveInputKey, GridCoverage> inputRDD =
        GeoWaveRDDLoader.loadRawRasterRDD(
            jsc.sc(),
            inputStoreOptions,
            index.getName(),
            rasterResizeOptions.getMinSplits(),
            rasterResizeOptions.getMaxSplits());
    LOGGER.debug("Writing results to output store...");
    RDDUtils.writeRasterToGeoWave(
        jsc.sc(),
        index,
        outputStoreOptions,
        newAdapter,
        inputRDD.flatMapToPair(
            new RasterResizeMappingFunction(
                internalAdapterId,
                newInternalAdapterId,
                newAdapter,
                index)).groupByKey().map(
                    new MergeRasterFunction(
                        internalAdapterId,
                        newInternalAdapterId,
                        newAdapter,
                        index)));

    LOGGER.debug("Results successfully written!");
  }

  private static class RasterResizeMappingFunction implements
      PairFlatMapFunction<Tuple2<GeoWaveInputKey, GridCoverage>, GeoWaveInputKey, GridCoverageWritable> {
    private final RasterTileResizeHelper helper;
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public RasterResizeMappingFunction(
        final short oldAdapterId,
        final short newAdapterId,
        final RasterDataAdapter newAdapter,
        final Index index) {
      super();
      helper = new RasterTileResizeHelper(oldAdapterId, newAdapterId, newAdapter, index);
    }

    @Override
    public Iterator<Tuple2<GeoWaveInputKey, GridCoverageWritable>> call(
        final Tuple2<GeoWaveInputKey, GridCoverage> t) throws Exception {

      if (helper.isOriginalCoverage(t._1.getInternalAdapterId())) {
        final Iterator<GridCoverage> coverages = helper.getCoveragesForIndex(t._2);
        if (coverages == null) {
          LOGGER.error("Couldn't get coverages instance, getCoveragesForIndex returned null");
          throw new IOException(
              "Couldn't get coverages instance, getCoveragesForIndex returned null");
        }
        return Iterators.transform(Iterators.filter(coverages, FitToIndexGridCoverage.class), c -> {
          // it should be a FitToIndexGridCoverage because it was just
          // converted above (filtered just in case)
          final byte[] partitionKey = c.getPartitionKey();
          final byte[] sortKey = c.getSortKey();
          final GeoWaveKey geowaveKey =
              new GeoWaveKeyImpl(
                  helper.getNewDataId(c),
                  t._1.getInternalAdapterId(),
                  partitionKey,
                  sortKey,
                  0);
          final GeoWaveInputKey inputKey =
              new GeoWaveInputKey(helper.getNewAdapterId(), geowaveKey, helper.getIndexName());
          return new Tuple2<>(inputKey, helper.getSerializer().toWritable(c));
        });
      }
      return Collections.emptyIterator();
    }
  }
  private static class MergeRasterFunction implements
      Function<Tuple2<GeoWaveInputKey, Iterable<GridCoverageWritable>>, GridCoverage> {
    private final RasterTileResizeHelper helper;
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public MergeRasterFunction(
        final short oldAdapterId,
        final short newAdapterId,
        final RasterDataAdapter newAdapter,
        final Index index) {
      super();
      helper = new RasterTileResizeHelper(oldAdapterId, newAdapterId, newAdapter, index);
    }

    @Override
    public GridCoverage call(final Tuple2<GeoWaveInputKey, Iterable<GridCoverageWritable>> tuple)
        throws Exception {
      return helper.getMergedCoverage(
          tuple._1,
          Iterables.transform(tuple._2, gcw -> helper.getSerializer().fromWritable(gcw)));
    }

  }
}
