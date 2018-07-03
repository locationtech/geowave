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
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.math.util.MathUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.junit.Assert;
import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.adapter.ServerMergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.SimpleAbstractMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
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
public class GeoWaveBasicRasterIT extends
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

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicRasterIT.class);
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
		LOGGER.warn("*    RUNNING GeoWaveBasicRasterIT       *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveBasicRasterIT         *");
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
		final double eastLon = 45;
		final double southLat = 0;
		final double northLat = 45;
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
		final double westLon = 45;
		final double eastLon = 47.8125;
		final double southLat = -47.8125;
		final double northLat = -45;

		ingestGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				summingNumRasters,
				new SummingMergeStrategy());

		ingestGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new SumAndAveragingMergeStrategy());

		ingestNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize,
				westLon,
				eastLon,
				southLat,
				northLat);

		queryGeneralPurpose(
				summingCoverageName,
				summingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				summingNumBands,
				summingNumRasters,
				new SummingExpectedValue());

		queryNoDataMergeStrategy(
				noDataCoverageName,
				noDataTileSize);

		queryGeneralPurpose(
				sumAndAveragingCoverageName,
				sumAndAveragingTileSize,
				westLon,
				eastLon,
				southLat,
				northLat,
				sumAndAveragingNumBands,
				sumAndAveragingNumRasters,
				new SumAndAveragingExpectedValue());

		TestUtils.deleteAll(dataStoreOptions);
	}

	private void ingestAndQueryNoDataMergeStrategy(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat )
			throws IOException {
		ingestNoDataMergeStrategy(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);
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
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat )
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
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			writer.write(RasterUtils.createCoverageTypeDouble(
					coverageName,
					westLon,
					eastLon,
					southLat,
					northLat,
					raster1));
			writer.write(RasterUtils.createCoverageTypeDouble(
					coverageName,
					westLon,
					eastLon,
					southLat,
					northLat,
					raster2));
		}
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
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
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
				writer.write(RasterUtils.createCoverageTypeDouble(
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
			final ExpectedValue expectedValue )
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
								northLat))))) {
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

	private static interface ExpectedValue
	{
		public double getExpectedValue(
				int x,
				int y,
				int b,
				int numRasters,
				int tileSize );
	}

	private static class SummingExpectedValue implements
			ExpectedValue
	{
		@Override
		public double getExpectedValue(
				final int x,
				final int y,
				final int b,
				final int numRasters,
				final int tileSize ) {
			double sum = 0;
			for (int r = 0; r < numRasters; r++) {
				sum += TestUtils.getTileValue(
						x,
						y,
						b,
						r,
						tileSize);
			}
			return sum;
		}
	}

	private static class SumAndAveragingExpectedValue implements
			ExpectedValue
	{
		@Override
		public double getExpectedValue(
				final int x,
				final int y,
				final int b,
				final int numRasters,
				final int tileSize ) {
			double sum = 0;
			final boolean isSum = ((b % 2) == 0);

			for (int r = 0; r < numRasters; r++) {
				sum += TestUtils.getTileValue(
						x,
						y,
						isSum ? b : b - 1,
						r,
						tileSize);
			}
			if (isSum) {
				return sum;
			}
			else {
				return sum / numRasters;
			}
		}
	}

	/**
	 * this will sum up every band
	 */
	public static class SummingMergeStrategy extends
			SimpleAbstractMergeStrategy<Persistable>
	{

		public SummingMergeStrategy() {
			super();
		}

		@Override
		protected double getSample(
				final int x,
				final int y,
				final int b,
				final double thisSample,
				final double nextSample ) {
			return thisSample + nextSample;
		}
	}

	/**
	 * this will sum up every even band and place the average of the previous
	 * band in each odd band
	 */
	public static class SumAndAveragingMergeStrategy implements
			RasterTileMergeStrategy<MergeCounter>
	{

		public SumAndAveragingMergeStrategy() {
			super();
		}

		@Override
		public void merge(
				final RasterTile<MergeCounter> thisTile,
				final RasterTile<MergeCounter> nextTile,
				final SampleModel sampleModel ) {
			if (nextTile instanceof ServerMergeableRasterTile) {
				final WritableRaster nextRaster = Raster.createWritableRaster(
						sampleModel,
						nextTile.getDataBuffer(),
						null);
				final WritableRaster thisRaster = Raster.createWritableRaster(
						sampleModel,
						thisTile.getDataBuffer(),
						null);
				final MergeCounter mergeCounter = thisTile.getMetadata();
				// we're merging, this is the incremented new number of merges
				final int newNumMerges = mergeCounter.getNumMerges() + nextTile.getMetadata().getNumMerges() + 1;

				// we've merged 1 more tile than the total number of merges (ie.
				// if we've performed 1 merge, we've seen 2 tiles)
				final int totalTiles = newNumMerges + 1;
				final int maxX = nextRaster.getMinX() + nextRaster.getWidth();
				final int maxY = nextRaster.getMinY() + nextRaster.getHeight();
				for (int x = nextRaster.getMinX(); x < maxX; x++) {
					for (int y = nextRaster.getMinY(); y < maxY; y++) {
						for (int b = 0; (b + 1) < nextRaster.getNumBands(); b += 2) {
							final double thisSample = thisRaster.getSampleDouble(
									x,
									y,
									b);
							final double nextSample = nextRaster.getSampleDouble(
									x,
									y,
									b);

							final double sum = thisSample + nextSample;
							final double average = sum / totalTiles;
							thisRaster.setSample(
									x,
									y,
									b,
									sum);
							thisRaster.setSample(
									x,
									y,
									b + 1,
									average);
						}
					}
				}
				thisTile.setMetadata(new MergeCounter(
						newNumMerges));
			}
		}

		@Override
		public MergeCounter getMetadata(
				final GridCoverage tileGridCoverage,
				final RasterDataAdapter dataAdapter ) {
			// initial merge counter
			return new MergeCounter();
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}

	public static class MergeCounter implements
			Persistable
	{
		private int mergeCounter = 0;

		public MergeCounter() {}

		protected MergeCounter(
				final int mergeCounter ) {
			this.mergeCounter = mergeCounter;
		}

		public int getNumMerges() {
			return mergeCounter;
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer buf = ByteBuffer.allocate(12);
			buf.putInt(mergeCounter);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			mergeCounter = buf.getInt();
		}
	}
}
