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
package mil.nga.giat.geowave.test.services;

import java.io.IOException;

import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.examples.ingest.SimpleIngest;
import mil.nga.giat.geowave.service.client.ConfigServiceClient;
import mil.nga.giat.geowave.service.client.RemoteServiceClient;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SERVICES
})
public class RemoteIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RemoteIT.class);
	private static ConfigServiceClient configServiceClient;
	private RemoteServiceClient remoteServiceClient;

	private String store_name = "test_store";

	private final static String testName = "RemoteIT";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB
	})
	protected DataStorePluginOptions dataStoreOptions;

	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		TestUtils.printStartOfTest(
				LOGGER,
				testName);
	}

	@AfterClass
	public static void reportTest() {
		TestUtils.printEndOfTest(
				LOGGER,
				testName,
				startMillis);
	}

	@Before
	public void initialize()
			throws MismatchedIndexToAdapterMapping,
			IOException {
		remoteServiceClient = new RemoteServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);
		configServiceClient = new ConfigServiceClient(
				ServicesTestEnvironment.GEOWAVE_BASE_URL);

		final DataStore ds = dataStoreOptions.createDataStore();
		final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
		final PrimaryIndex idx = SimpleIngest.createSpatialIndex();
		final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
		final List<SimpleFeature> features = SimpleIngest.getGriddedFeatures(
				new SimpleFeatureBuilder(
						sft),
				8675309);
		LOGGER.info(String.format(
				"Beginning to ingest a uniform grid of %d features",
				features.size()));
		int ingestedFeatures = 0;
		final int featuresPer5Percent = features.size() / 20;
		try (IndexWriter writer = ds.createWriter(
				fda,
				idx)) {
			for (final SimpleFeature feat : features) {
				writer.write(feat);
				ingestedFeatures++;
				if ((ingestedFeatures % featuresPer5Percent) == 0) {
					LOGGER.info(String.format(
							"Ingested %d percent of features",
							(ingestedFeatures / featuresPer5Percent) * 5));
				}
			}
		}
		configServiceClient.addStoreReRoute(
				store_name,
				dataStoreOptions.getType(),
				TestUtils.TEST_NAMESPACE,
				dataStoreOptions.getOptionsAsMap());
	}

	@After
	public void cleanupWorkspace() {
		configServiceClient.removeStore(store_name);
	}

	@Test
	public void testCalcStat() {
		TestUtils.assertStatusCode(
				"Should successfully calculate stat for existent store, adapterID, and statID",
				200,
				remoteServiceClient.calcStat(
						store_name,
						"GridPoint",
						"COUNT_DATA"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for calculate stat for existent store and adapterID, but a nonexistent statID.  A warning is output",
						200,
						remoteServiceClient.calcStat(
								store_name,
								"GridPoint",
								"nonexistent-stat"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for calculate stat for existent store and statID, but nonexistent adapterID.  A warning is output",
						200,
						remoteServiceClient.calcStat(
								store_name,
								"nonexistent-adapter",
								"COUNT_DATA"));

		TestUtils.assertStatusCode(
				"Should fail to calculate stat for existent adapterID and statID, but nonexistent store",
				400,
				remoteServiceClient.calcStat(
						"nonexistent-store",
						"GridPoint",
						"COUNT_DATA"));
	}

	@Test
	public void testClear() {
		TestUtils.assertStatusCode(
				"Should successfully clear for existent store",
				200,
				remoteServiceClient.clear(store_name));

		TestUtils.assertStatusCode(
				"Should fail to clear for nonexistent store",
				400,
				remoteServiceClient.clear("nonexistent-store"));
	}

	@Test
	public void testListAdapter() {
		TestUtils.assertStatusCode(
				"Should successfully list adapters for existent store",
				200,
				remoteServiceClient.listAdapter(store_name));

		TestUtils.assertStatusCode(
				"Should fail to list adapters for nonexistent store",
				400,
				remoteServiceClient.listAdapter("nonexistent-store"));
	}

	@Test
	public void testListIndex() {
		TestUtils.assertStatusCode(
				"Should successfully list indices for existent store",
				200,
				remoteServiceClient.listIndex(store_name));

		TestUtils.assertStatusCode(
				"Should fail to list indices for nonexistent store",
				400,
				remoteServiceClient.listIndex("nonexistent-store"));
	}

	@Test
	public void testListStats() {
		TestUtils.assertStatusCode(
				"Should successfully liststats for existent store",
				200,
				remoteServiceClient.listStats(store_name));

		TestUtils.assertStatusCode(
				"Should fail to liststats for nonexistent store",
				400,
				remoteServiceClient.listStats("nonexistent-store"));
	}

	@Test
	public void testRecalcStats() {
		TestUtils.assertStatusCode(
				"Should successfully recalc stats for existent store",
				200,
				remoteServiceClient.recalcStats(store_name));

		TestUtils.assertStatusCode(
				"Should successfully recalc stats for existent store and existent adapter",
				200,
				remoteServiceClient.recalcStats(
						store_name,
						"GridPoint",
						null,
						null));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils.assertStatusCode(
				"Returns a successful 200 status for recalc stats for existent store and nonexistent adapter",
				200,
				remoteServiceClient.recalcStats(
						store_name,
						"nonexistent-adapter",
						null,
						null));

		TestUtils.assertStatusCode(
				"Should fail to recalc stats for nonexistent store",
				400,
				remoteServiceClient.recalcStats("nonexistent-store"));
	}

	@Test
	public void testRemoveAdapter() {
		TestUtils.assertStatusCode(
				"Should successfully remove adapter for existent store and existent adapter",
				200,
				remoteServiceClient.removeAdapter(
						store_name,
						"GridPoint"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for removing adapter for existent store and previously removed adapter.  A warning is output",
						200,
						remoteServiceClient.removeAdapter(
								store_name,
								"GridPoint"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for removing adapter for existent store and nonexistent adapter.  A warning is output",
						200,
						remoteServiceClient.removeAdapter(
								store_name,
								"nonexistent-adapter"));

		TestUtils.assertStatusCode(
				"Should fail to remove adapter for nonexistent store",
				400,
				remoteServiceClient.removeAdapter(
						"nonexistent-store",
						"GridPoint"));
	}

	@Test
	public void testRemoveStat() {
		TestUtils.assertStatusCode(
				"Should successfully remove stat for existent store, adapterID, and statID",
				200,
				remoteServiceClient.removeStat(
						store_name,
						"GridPoint",
						"COUNT_DATA"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for removing stat for existent store and adapterID, but a nonexistent statID.  A warning is output.",
						200,
						remoteServiceClient.removeStat(
								store_name,
								"GridPoint",
								"nonexistent-stat"));

		// The following case should probably return a 404 based on the
		// situation described in the test description
		TestUtils
				.assertStatusCode(
						"Returns a successful 200 status for removing stat for existent store and statID, but nonexistent adapterID.  A warning is output",
						200,
						remoteServiceClient.removeStat(
								store_name,
								"nonexistent-adapter",
								"COUNT_DATA"));

		TestUtils.assertStatusCode(
				"Should fail to remove for existent adapterID and statID, but nonexistent store",
				400,
				remoteServiceClient.removeStat(
						"nonexistent-store",
						"GridPoint",
						"COUNT_DATA"));
	}

	@Test
	public void testVersion() {
		TestUtils.assertStatusCode(
				"Should successfully return version for existent store",
				200,
				remoteServiceClient.version(store_name));

		TestUtils.assertStatusCode(
				"Should fail to return version for nonexistent store",
				400,
				remoteServiceClient.version("nonexistent-store"));
	}
}
