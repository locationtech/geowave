/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.util.Stopwatch;

@RunWith(GeoWaveITRunner.class)
public class GeowaveBasicURLIngestIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeowaveBasicURLIngestIT.class);

	private final static String S3URL = "s3.amazonaws.com";
	protected static final String GDELT_INPUT_FILE_URL = "s3://geowave-test/data/gdelt/20160202.export.CSV.zip";
	private static final int GDELT_URL_COUNT = 224482;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.REDIS
	})
	protected DataStorePluginOptions dataStore;

	private static Stopwatch stopwatch = new Stopwatch();

	@BeforeClass
	public static void reportTestStart() {
		stopwatch.reset();
		stopwatch.start();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeowaveBasicURLIngestIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		stopwatch.stop();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeowaveBasicURLIngestIT           *");
		LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testBasicURLIngest()
			throws Exception {

		TestUtils.testS3LocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				S3URL,
				GDELT_INPUT_FILE_URL,
				"gdelt",
				4);

		final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		try (CloseableIterator<InternalDataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final InternalDataAdapter<?> internalDataAdapter = adapterIterator.next();
				final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();

				// query by the full bounding box, make sure there is more than
				// 0 count and make sure the count matches the number of results
				final StatisticsId statsId = VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
						adapter.getFeatureType().getGeometryDescriptor().getLocalName()).build().getId();
				try (final CloseableIterator<BoundingBoxDataStatistics<?>> bboxStatIt = (CloseableIterator) statsStore
						.getDataStatistics(
								internalDataAdapter.getAdapterId(),
								statsId.getExtendedId(),
								statsId.getType())) {
					final BoundingBoxDataStatistics<?> bboxStat = bboxStatIt.next();
					try (final CloseableIterator<CountDataStatistics<SimpleFeature>> countStatIt = (CloseableIterator) statsStore
							.getDataStatistics(
									internalDataAdapter.getAdapterId(),
									CountDataStatistics.STATS_TYPE)) {
						final CountDataStatistics<?> countStat = countStatIt.next();
						// then query it
						final GeometryFactory factory = new GeometryFactory();
						final Envelope env = new Envelope(
								bboxStat.getMinX(),
								bboxStat.getMaxX(),
								bboxStat.getMinY(),
								bboxStat.getMaxY());
						final Geometry spatialFilter = factory.toGeometry(env);
						final QueryConstraints query = new SpatialQuery(
								spatialFilter);
						final int resultCount = testQuery(
								adapter,
								query);
						assertTrue(
								"'" + adapter.getTypeName()
										+ "' adapter must have at least one element in its statistic",
								countStat.getCount() > 0);
						assertEquals(
								"'" + adapter.getTypeName()
										+ "' adapter should have the same results from a spatial query of '" + env
										+ "' as its total count statistic",
								countStat.getCount(),
								resultCount);

						assertEquals(
								"'" + adapter.getTypeName()
										+ "' adapter entries ingested does not match expected count",
								new Integer(
										GDELT_URL_COUNT),
								new Integer(
										resultCount));
					}
				}
			}
		}

		// Clean up
		TestUtils.deleteAll(dataStore);

	}

	private int testQuery(
			final DataTypeAdapter<?> adapter,
			final QueryConstraints query )
			throws Exception {
		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = dataStore.createDataStore();

		final CloseableIterator<?> accumuloResults = geowaveStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				TestUtils.DEFAULT_SPATIAL_INDEX.getName()).constraints(
				query).build());

		int resultCount = 0;
		while (accumuloResults.hasNext()) {
			accumuloResults.next();

			resultCount++;
		}
		accumuloResults.close();

		return resultCount;

	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
