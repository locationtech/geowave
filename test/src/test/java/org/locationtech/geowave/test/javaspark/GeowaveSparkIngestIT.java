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
package org.locationtech.geowave.test.javaspark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataAdapter;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.DistributableQuery;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.util.Stopwatch;

@RunWith(GeoWaveITRunner.class)
public class GeowaveSparkIngestIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeowaveSparkIngestIT.class);
	private final static String S3URL = "s3.amazonaws.com";
	protected static final String GDELT_INPUT_FILES = "s3://geowave-test/data/gdelt";
	private static final int GDELT_COUNT = 448675;

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB
	})
	protected DataStorePluginOptions dataStore;

	private static Stopwatch stopwatch = new Stopwatch();

	@BeforeClass
	public static void reportTestStart() {
		stopwatch.reset();
		stopwatch.start();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveJavaSparkIngestIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		stopwatch.stop();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveJavaSparkIngestIT           *");
		LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testBasicSparkIngest()
			throws Exception {

		// ingest test points
		TestUtils.testSparkIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				S3URL,
				GDELT_INPUT_FILES,
				"gdelt");

		final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
		final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
		int adapterCount = 0;
		try (CloseableIterator<InternalDataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final InternalDataAdapter<?> internalDataAdapter = adapterIterator.next();
				final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();

				// query by the full bounding box, make sure there is more than
				// 0 count and make sure the count matches the number of results
				final BoundingBoxDataStatistics<?> bboxStat = (BoundingBoxDataStatistics<SimpleFeature>) statsStore
						.getDataStatistics(
								internalDataAdapter.getInternalAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getFeatureType()
										.getGeometryDescriptor()
										.getLocalName()));
				final CountDataStatistics<?> countStat = (CountDataStatistics<SimpleFeature>) statsStore
						.getDataStatistics(
								internalDataAdapter.getInternalAdapterId(),
								CountDataStatistics.STATS_TYPE);
				// then query it
				final GeometryFactory factory = new GeometryFactory();
				final Envelope env = new Envelope(
						bboxStat.getMinX(),
						bboxStat.getMaxX(),
						bboxStat.getMinY(),
						bboxStat.getMaxY());
				final Geometry spatialFilter = factory.toGeometry(env);
				final Query query = new SpatialQuery(
						spatialFilter);
				final int resultCount = testQuery(
						adapter,
						query);
				assertTrue(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter must have at least one element in its statistic",
						countStat.getCount() > 0);
				assertEquals(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter should have the same results from a spatial query of '" + env
								+ "' as its total count statistic",
						countStat.getCount(),
						resultCount);

				assertEquals(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter entries ingested does not match expected count",
						new Integer(
								GDELT_COUNT),
						new Integer(
								resultCount));

				adapterCount++;
			}
		}

		// Clean up
		TestUtils.deleteAll(dataStore);

	}

	private int testQuery(
			final DataAdapter<?> adapter,
			final Query query )
			throws Exception {
		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = dataStore.createDataStore();

		final CloseableIterator<?> accumuloResults = geowaveStore.query(
				new QueryOptions(
						adapter,
						TestUtils.DEFAULT_SPATIAL_INDEX),
				query);

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
