/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test.basic;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;

import org.apache.commons.math.util.MathUtils;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicCustomCRSRasterIT extends
		AbstractGeoWaveIT
{
	private static final double DOUBLE_TOLERANCE = 1E-10d;
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicCustomCRSRasterIT.class);
	private static final double DELTA = MathUtils.EPSILON;
	private static long startMillis;

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStoreOptions;
	}

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    RUNNING GeoWaveBasicCustomCRSRasterIT       *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveBasicCustomCRSRasterIT         *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testNoDataMergeStrategy()
			throws IOException {
		final String coverageName = "testNoDataMergeStrategy";
		final int tileSize = 64; // 256 fails on bigtable exceeding maximum
									// size, 128 fails on DynamoDB exceeding
									// maximum size
		final double westLon = 0;
		final double eastLon = 62500;
		final double southLat = 0;
		final double northLat = 62500;
		ingestAndQueryNoDataMergeStrategy(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);
		TestUtils.deleteAll(dataStoreOptions);
	}

	@Test
	public void testMultipleMergeStrategies()
			throws IOException {
		final String noDataCoverageName = "testMultipleMergeStrategies_NoDataMergeStrategy";
		final String summingCoverageName = "testMultipleMergeStrategies_SummingMergeStrategy";
		final String sumAndAveragingCoverageName = "testMultipleMergeStrategies_SumAndAveragingMergeStrategy";
		final int summingNumBands = 8;
		final int summingNumRasters = 4;

		final int sumAndAveragingNumBands = 12;
		final int sumAndAveragingNumRasters = 15;
		final int noDataTileSize = 64;
		final int summingTileSize = 32;
		final int sumAndAveragingTileSize = 8;
		final double minX = 0;
		final double maxX = 244.140625;
		final double minY = 0;
		final double maxY = 244.140625;

		ingestGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				minX,
				maxX,
				minY,
				maxY,
				summingNumBands,
				summingNumRasters,
				new GeoWaveBasicRasterIT.SummingMergeStrategy());

		ingestGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				minX,
				maxX,
				minY,
				maxY,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new GeoWaveBasicRasterIT.SumAndAveragingMergeStrategy());

		ingestNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize,
				minX,
				maxX,
				minY,
				maxY);

		queryGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				minX,
				maxX,
				minY,
				maxY,
				summingNumBands,
				summingNumRasters,
				new GeoWaveBasicRasterIT.SummingExpectedValue());

		queryNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize);

		queryGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				minX,
				maxX,
				minY,
				maxY,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new GeoWaveBasicRasterIT.SumAndAveragingExpectedValue());

		TestUtils.deleteAll(dataStoreOptions);
	}

	private void ingestAndQueryNoDataMergeStrategy(
			final String coverageName,
			final int tileSize,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY )
			throws IOException {
		ingestNoDataMergeStrategy(
				coverageName,
				tileSize,
				minX,
				maxX,
				minY,
				maxY);
		queryNoDataMergeStrategy(
				coverageName,
				tileSize);
	}

	private void queryNoDataMergeStrategy(
			final String coverageName,
			final int tileSize )
			throws IOException {
		final DataStore dataStore = dataStoreOptions.createDataStore();

		try (CloseableIterator<?> it = dataStore.query(
				new QueryOptions(
						new ByteArrayId(
								coverageName),
						null),
				new EverythingQuery())) {

			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth(),
					DELTA);
			Assert.assertEquals(
					tileSize,
					raster.getHeight(),
					DELTA);
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {

					for (int b = 1; b < 7; b++) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								TestUtils.getTileValue(
										x,
										y,
										b,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										b),
								DELTA);

					}
					if ((y % 2) == 0) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								TestUtils.getTileValue(
										x,
										y,
										0,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										0),
								DELTA);
					}
					else {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								Double.NaN,
								raster.getSampleDouble(
										x,
										y,
										0),
								DELTA);
					}
					if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								Double.NaN,
								raster.getSampleDouble(
										x,
										y,
										7),
								DELTA);
					}
					else {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								TestUtils.getTileValue(
										x,
										y,
										7,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										7),
								DELTA);

					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

	private void ingestNoDataMergeStrategy(
			final String coverageName,
			final int tileSize,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY )
			throws IOException {
		final int numBands = 8;
		final DataStore dataStore = dataStoreOptions.createDataStore();
		final RasterDataAdapter adapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize,
				new NoDataMergeStrategy());
		final WritableRaster raster1 = RasterUtils.createRasterTypeDouble(
				numBands,
				tileSize);
		final WritableRaster raster2 = RasterUtils.createRasterTypeDouble(
				numBands,
				tileSize);

		TestUtils.fillTestRasters(
				raster1,
				raster2,
				tileSize);
		try (IndexWriter writer = dataStore.createWriter(
				adapter,
				TestUtils.createCustomCRSPrimaryIndex())) {
			writer.write(createCoverageTypeDouble(
					coverageName,
					minX,
					maxX,
					minY,
					maxY,
					raster1));
			writer.write(createCoverageTypeDouble(
					coverageName,
					minX,
					maxX,
					minY,
					maxY,
					raster2));
		}
	}

	private static GridCoverage2D createCoverageTypeDouble(
			final String coverageName,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY,
			final WritableRaster raster ) {
		final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
		final org.opengis.geometry.Envelope mapExtent = new ReferencedEnvelope(
				minX,
				maxX,
				minY,
				maxY,
				TestUtils.CUSTOM_CRS);
		return gcf.create(
				coverageName,
				raster,
				mapExtent);
	}

	private void ingestGeneralPurpose(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final int numBands,
			final int numRasters,
			final RasterTileMergeStrategy<?> mergeStrategy )
			throws IOException {

		// just ingest a number of rasters
		final DataStore dataStore = dataStoreOptions.createDataStore();
		final RasterDataAdapter basicAdapter = RasterUtils.createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize,
				new NoDataMergeStrategy());
		final RasterDataAdapter mergeStrategyOverriddenAdapter = new RasterDataAdapter(
				basicAdapter,
				coverageName,
				mergeStrategy);
		basicAdapter.getMetadata().put(
				"test-key",
				"test-value");
		try (IndexWriter writer = dataStore.createWriter(
				mergeStrategyOverriddenAdapter,
				// TestUtils.DEFAULT_SPATIAL_INDEX
				TestUtils.createCustomCRSPrimaryIndex())) {
			for (int r = 0; r < numRasters; r++) {
				final WritableRaster raster = RasterUtils.createRasterTypeDouble(
						numBands,
						tileSize);
				for (int x = 0; x < tileSize; x++) {
					for (int y = 0; y < tileSize; y++) {
						for (int b = 0; b < numBands; b++) {
							raster.setSample(
									x,
									y,
									b,
									TestUtils.getTileValue(
											x,
											y,
											b,
											r,
											tileSize));
						}
					}
				}
				writer.write(createCoverageTypeDouble(
						coverageName,
						westLon,
						eastLon,
						southLat,
						northLat,
						raster));
			}
		}
	}

	private void queryGeneralPurpose(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final int numBands,
			final int numRasters,
			final GeoWaveBasicRasterIT.ExpectedValue expectedValue )
			throws IOException {
		final DataStore dataStore = dataStoreOptions.createDataStore();

		try (CloseableIterator<?> it = dataStore.query(
				new QueryOptions(
						new ByteArrayId(
								coverageName),
						null),
				new IndexOnlySpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								westLon,
								eastLon,
								southLat,
								northLat)),
						TestUtils.CUSTOM_CRSCODE))) {
			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 0; b < numBands; b++) {
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								expectedValue.getExpectedValue(
										x,
										y,
										b,
										numRasters,
										tileSize),
								raster.getSampleDouble(
										x,
										y,
										b),
								DOUBLE_TOLERANCE);

					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

}
