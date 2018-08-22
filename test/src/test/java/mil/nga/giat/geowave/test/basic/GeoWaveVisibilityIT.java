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
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.raster.RasterUtils;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveVisibilityIT extends
		AbstractGeoWaveIT
{
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.DYNAMODB
	})
	protected DataStorePluginOptions dataStoreOptions;

	private static final Logger LOGGER = Logger.getLogger(AbstractGeoWaveIT.class);
	private static long startMillis;

	private static final int TOTAL_FEATURES = 800;

	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStoreOptions;
	}

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveVisibilityIT   *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveVisibilityIT     *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testIngestAndQueryMixedVisibilityRasters()
			throws IOException {
		final String coverageName = "testMixedVisibilityRasters";
		final int tileSize = 64;
		final double westLon = 0;
		final double eastLon = 45;
		final double southLat = 0;
		final double northLat = 45;

		ingestAndQueryMixedVisibilityRasters(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);

		TestUtils.deleteAll(dataStoreOptions);
	}

	@Test
	public void testComplexVisibility()
			throws IOException {
		final String coverageName = "testComplexVisibility";
		final int tileSize = 64;
		final double westLon = 0;
		final double eastLon = 45;
		final double southLat = 0;
		final double northLat = 45;

		ingestAndQueryComplexVisibilityRasters(
				coverageName,
				tileSize,
				westLon,
				eastLon,
				southLat,
				northLat);

		TestUtils.deleteAll(dataStoreOptions);
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	private void ingestAndQueryComplexVisibilityRasters(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat )
			throws IOException {
		// Create two test rasters
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
			// Write the first raster w/ vis info
			writer.write(
					RasterUtils.createCoverageTypeDouble(
							coverageName,
							westLon,
							eastLon,
							southLat,
							northLat,
							raster1),
					getRasterVisWriter("(a&b)|c"));

			// Write the second raster w/ no vis info
			writer.write(RasterUtils.createCoverageTypeDouble(
					coverageName,
					westLon,
					eastLon,
					southLat,
					northLat,
					raster2));
		}

		// First, query w/ no authorizations. We should get
		// just the second raster back
		QueryOptions queryOptions = new QueryOptions(
				new ByteArrayId(
						coverageName),
				null);

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

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
						double p0 = raster.getSampleDouble(
								x,
								y,
								b);
						double p1 = raster2.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								p0,
								p1,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}

		// Next, query w/ only 'a' authorization. We should get
		// just the second raster back
		queryOptions.setAuthorizations(new String[] {
			"a",
		});

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

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
						double p0 = raster.getSampleDouble(
								x,
								y,
								b);
						double p1 = raster2.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								p0,
								p1,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}

		// Next, query w/ only 'b' authorization. We should get
		// just the second raster back
		queryOptions.setAuthorizations(new String[] {
			"b"
		});

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

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
						double p0 = raster.getSampleDouble(
								x,
								y,
								b);
						double p1 = raster2.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								p0,
								p1,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}

		// Now, query w/ only "c" authorization. We should get
		// just the merged raster back
		queryOptions.setAuthorizations(new String[] {
			"c"
		});

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());

			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 1; b < 7; b++) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								b,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								pExp,
								pAct,
								0.0);
					}
					if ((y % 2) == 0) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								0,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								pExp,
								pAct,
								0.0);
					}
					else {
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								Double.NaN,
								pAct,
								0.0);
					}
					if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								Double.NaN,
								pAct,
								0.0);
					}
					else {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								7,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								pExp,
								pAct,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}

		// Finally, query w/ "a" and "b" authorization. We should get
		// just the merged raster back
		queryOptions.setAuthorizations(new String[] {
			"a",
			"b"
		});

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());

			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 1; b < 7; b++) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								b,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								pExp,
								pAct,
								0.0);
					}
					if ((y % 2) == 0) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								0,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								pExp,
								pAct,
								0.0);
					}
					else {
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								Double.NaN,
								pAct,
								0.0);
					}
					if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								Double.NaN,
								pAct,
								0.0);
					}
					else {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								7,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								pExp,
								pAct,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

	private void ingestAndQueryMixedVisibilityRasters(
			final String coverageName,
			final int tileSize,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat )
			throws IOException {
		// Create two test rasters
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
			// Write the first raster w/ vis info
			writer.write(
					RasterUtils.createCoverageTypeDouble(
							coverageName,
							westLon,
							eastLon,
							southLat,
							northLat,
							raster1),
					getRasterVisWriter("a"));

			// Write the second raster w/ no vis info
			writer.write(RasterUtils.createCoverageTypeDouble(
					coverageName,
					westLon,
					eastLon,
					southLat,
					northLat,
					raster2));
		}

		// First, query w/ no authorizations. We should get
		// just the second raster back
		QueryOptions queryOptions = new QueryOptions(
				new ByteArrayId(
						coverageName),
				null);

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

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
						double p0 = raster.getSampleDouble(
								x,
								y,
								b);
						double p1 = raster2.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								p0,
								p1,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}

		// Now, query w/ authorization. We should get
		// just the merged raster back
		String[] auths = {
			"a"
		};
		queryOptions.setAuthorizations(auths);

		try (CloseableIterator<?> it = dataStore.query(
				queryOptions,
				new EverythingQuery())) {

			final GridCoverage coverage = (GridCoverage) it.next();
			final Raster raster = coverage.getRenderedImage().getData();

			Assert.assertEquals(
					tileSize,
					raster.getWidth());
			Assert.assertEquals(
					tileSize,
					raster.getHeight());

			// the expected outcome is:
			// band 1,2,3,4,5,6 has every value set correctly, band 0 has every
			// even row set correctly and every odd row should be NaN, and band
			// 7 has the upper quadrant as NaN and the rest set
			for (int x = 0; x < tileSize; x++) {
				for (int y = 0; y < tileSize; y++) {
					for (int b = 1; b < 7; b++) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								b,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								b);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=" + b,
								pExp,
								pAct,
								0.0);
					}
					if ((y % 2) == 0) {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								0,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);

						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								pExp,
								pAct,
								0.0);
					}
					else {
						double pAct = raster.getSampleDouble(
								x,
								y,
								0);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=0",
								Double.NaN,
								pAct,
								0.0);
					}
					if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								Double.NaN,
								pAct,
								0.0);
					}
					else {
						double pExp = TestUtils.getTileValue(
								x,
								y,
								7,
								tileSize);
						double pAct = raster.getSampleDouble(
								x,
								y,
								7);
						Assert.assertEquals(
								"x=" + x + ",y=" + y + ",b=7",
								pExp,
								pAct,
								0.0);
					}
				}
			}

			// there should be exactly one
			Assert.assertFalse(it.hasNext());
		}
	}

	@Test
	public void testIngestAndQueryMixedVisibilityFields()
			throws MismatchedIndexToAdapterMapping,
			IOException {
		final SimpleFeatureBuilder bldr = new SimpleFeatureBuilder(
				getType());
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				getType());
		final DataStore store = dataStoreOptions.createDataStore();
		try (IndexWriter writer = store.createWriter(
				adapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			for (int i = 0; i < TOTAL_FEATURES; i++) {
				bldr.set(
						"a",
						Integer.toString(i));
				bldr.set(
						"b",
						Integer.toString(i));
				bldr.set(
						"c",
						Integer.toString(i));
				bldr.set(
						"geometry",
						new GeometryFactory().createPoint(new Coordinate(
								0,
								0)));
				writer.write(
						bldr.buildFeature(Integer.toString(i)),
						getFeatureVisWriter());
			}
		}
		final InternalAdapterStore internalDataStore = dataStoreOptions.createInternalAdapterStore();
		short internalAdapterId = internalDataStore.getInternalAdapterId(adapter.getAdapterId());
		final DifferingFieldVisibilityEntryCount differingVisibilities = (DifferingFieldVisibilityEntryCount) dataStoreOptions
				.createDataStatisticsStore()
				.getDataStatistics(
						internalAdapterId,
						DifferingFieldVisibilityEntryCount.composeId(TestUtils.DEFAULT_SPATIAL_INDEX.getId()));
		Assert.assertEquals(
				"Exactly half the entries should have differing visibility",
				TOTAL_FEATURES / 2,
				differingVisibilities.getEntriesWithDifferingFieldVisibilities());
		testQueryMixed(
				store,
				false);
		testQueryMixed(
				store,
				true);
		TestUtils.deleteAll(dataStoreOptions);
	}

	private VisibilityWriter<SimpleFeature> getFeatureVisWriter() {
		return new VisibilityWriter<SimpleFeature>() {
			@Override
			public FieldVisibilityHandler<SimpleFeature, Object> getFieldVisibilityHandler(
					final ByteArrayId fieldId ) {
				return new FieldVisibilityHandler<SimpleFeature, Object>() {

					@Override
					public byte[] getVisibility(
							final SimpleFeature rowValue,
							final ByteArrayId fieldId,
							final Object fieldValue ) {

						final boolean isGeom = fieldId.equals(GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
						final int fieldValueInt;
						if (isGeom) {
							fieldValueInt = Integer.parseInt(rowValue.getID());
						}
						else {
							fieldValueInt = Integer.parseInt(fieldValue.toString());
						}
						// just make half of them varied and
						// half of them the same
						if ((fieldValueInt % 2) == 0) {
							if (isGeom) {
								return new byte[] {};
							}
							return fieldId.getBytes();
						}
						else {
							// of the ones that are the same,
							// make some no bytes, some a, some
							// b, and some c
							final int switchValue = (fieldValueInt / 2) % 4;
							switch (switchValue) {
								case 0:
									return new ByteArrayId(
											"a").getBytes();

								case 1:
									return new ByteArrayId(
											"b").getBytes();

								case 2:
									return new ByteArrayId(
											"c").getBytes();

								case 3:
								default:
									return new byte[] {};
							}
						}
					}
				};
			}
		};
	}

	private VisibilityWriter<GridCoverage> getRasterVisWriter(
			String visExpression ) {
		return new VisibilityWriter<GridCoverage>() {
			@Override
			public FieldVisibilityHandler<GridCoverage, Object> getFieldVisibilityHandler(
					ByteArrayId fieldId ) {
				return new FieldVisibilityHandler<GridCoverage, Object>() {
					@Override
					public byte[] getVisibility(
							GridCoverage rowValue,
							ByteArrayId fieldId,
							Object fieldValue ) {
						return new ByteArrayId(
								visExpression).getBytes();
					}

				};
			}

		};
	}

	private static void testQueryMixed(
			final DataStore store,
			boolean spatial )
			throws IOException {

		// you have to at least be able to see the geometry field which is wide
		// open for exactly (5 * total_Features / 8)
		// for other fields there is exactly
		testQuery(
				store,
				new String[] {},
				spatial,
				(5 * TOTAL_FEATURES) / 8,
				((TOTAL_FEATURES / 8) * 4) + (TOTAL_FEATURES / 2));

		for (String auth : new String[] {
			"a",
			"b",
			"c"
		}) {
			testQuery(
					store,
					new String[] {
						auth
					},
					spatial,
					(6 * TOTAL_FEATURES) / 8,
					((2 * TOTAL_FEATURES / 8) * 4) + (2 * TOTAL_FEATURES / 2));
		}

		// order shouldn't matter, but let's make sure here
		for (String[] auths : new String[][] {
			new String[] {
				"a",
				"b"
			},
			new String[] {
				"b",
				"a"
			},
			new String[] {
				"a",
				"c"
			},
			new String[] {
				"c",
				"a"
			},
			new String[] {
				"b",
				"c"
			},
			new String[] {
				"c",
				"b"
			}
		}) {
			testQuery(
					store,
					auths,
					spatial,
					(7 * TOTAL_FEATURES) / 8,
					((3 * TOTAL_FEATURES / 8) * 4) + (3 * TOTAL_FEATURES / 2));
		}

		testQuery(
				store,
				new String[] {
					"a",
					"b",
					"c"
				},
				spatial,
				TOTAL_FEATURES,
				TOTAL_FEATURES * 4);
	}

	private static void testQuery(
			final DataStore store,
			final String[] auths,
			final boolean spatial,
			final int expectedResultCount,
			final int expectedNonNullFieldCount )
			throws IOException {
		final QueryOptions queryOpts = new QueryOptions();
		queryOpts.setAuthorizations(auths);
		try (CloseableIterator<SimpleFeature> it = (CloseableIterator) store.query(
				queryOpts,
				spatial ? new SpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								-1,
								1,
								-1,
								1))) : null)) {
			int resultCount = 0;
			int nonNullFieldsCount = 0;
			while (it.hasNext()) {
				final SimpleFeature feature = it.next();
				for (int a = 0; a < feature.getAttributeCount(); a++) {
					if (feature.getAttribute(a) != null) {
						nonNullFieldsCount++;
					}
				}
				resultCount++;
			}
			Assert.assertEquals(
					"Unexpected result count for " + (spatial ? "spatial query" : "full table scan") + " with auths "
							+ Arrays.toString(auths),
					expectedResultCount,
					resultCount);

			Assert.assertEquals(
					"Unexpected non-null field count for " + (spatial ? "spatial query" : "full table scan")
							+ " with auths " + Arrays.toString(auths),
					expectedNonNullFieldCount,
					nonNullFieldsCount);
		}

		queryOpts.setAggregation(
				new CountAggregation(),
				new FeatureDataAdapter(
						getType()));
		try (CloseableIterator<CountResult> it = (CloseableIterator) store.query(
				queryOpts,
				spatial ? new SpatialQuery(
						new GeometryFactory().toGeometry(new Envelope(
								-1,
								1,
								-1,
								1))) : null)) {
			CountResult result = it.next();
			long count = 0;
			if (result != null) {
				count = result.getCount();
			}
			Assert.assertEquals(
					"Unexpected aggregation result count for " + (spatial ? "spatial query" : "full table scan")
							+ " with auths " + Arrays.toString(auths),
					expectedResultCount,
					count);
		}
	}

	private static SimpleFeatureType getType() {
		final SimpleFeatureTypeBuilder bldr = new SimpleFeatureTypeBuilder();
		bldr.setName("testvis");
		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"a"));
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"b"));
		bldr.add(attributeTypeBuilder.binding(
				String.class).buildDescriptor(
				"c"));
		bldr.add(attributeTypeBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		return bldr.buildFeatureType();
	}
}
