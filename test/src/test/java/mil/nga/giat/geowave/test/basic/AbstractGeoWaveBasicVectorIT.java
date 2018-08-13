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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.util.MathUtils;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.adapter.statistics.PartitionStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.ExpectedResults;

abstract public class AbstractGeoWaveBasicVectorIT extends
		AbstractGeoWaveIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveBasicVectorIT.class);
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
	protected static final String TEST_FILTER_PACKAGE = TestUtils.TEST_CASE_BASE + "filter/";
	protected static final String HAIL_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE + "hail_test_case/";
	protected static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
	protected static final String TORNADO_TRACKS_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE
			+ "tornado_tracks_test_case/";
	protected static final String TORNADO_TRACKS_SHAPEFILE_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks.shp";

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						GeoWaveBasicSpatialVectorIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);
	}

	protected void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final String queryDescription )
			throws Exception {
		// test the query with an unspecified index
		testQuery(
				savedFilterResource,
				expectedResultsResources,
				null,
				queryDescription);
	}

	protected void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final PrimaryIndex index,
			final String queryDescription )
			throws Exception {
		testQuery(
				savedFilterResource,
				expectedResultsResources,
				index,
				queryDescription,
				null);
	}

	protected void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final PrimaryIndex index,
			final String queryDescription,
			final CoordinateReferenceSystem crs )
			throws Exception {
		LOGGER.info("querying " + queryDescription);
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = getDataStorePluginOptions().createDataStore();
		// this file is the filtered dataset (using the previous file as a
		// filter) so use it to ensure the query worked
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);
		try (final CloseableIterator<?> actualResults = (index == null) ? geowaveStore.query(
				new QueryOptions(),
				query) : geowaveStore.query(
				new QueryOptions(
						index),
				query)) {
			final ExpectedResults expectedResults = TestUtils.getExpectedResults(
					expectedResultsResources,
					crs);
			int totalResults = 0;
			final List<Long> actualCentroids = new ArrayList<Long>();
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;
					final long actualHashCentroid = TestUtils.hashCentroid((Geometry) result.getDefaultGeometry());
					Assert.assertTrue(
							"Actual result '" + result.toString() + "' not found in expected result set",
							expectedResults.hashedCentroids.contains(actualHashCentroid));
					actualCentroids.add(actualHashCentroid);
					totalResults++;
				}
				else {
					TestUtils.deleteAll(getDataStorePluginOptions());
					Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
				}
			}
			for (final long l : actualCentroids) {
				expectedResults.hashedCentroids.remove(l);
			}
			for (final long l : expectedResults.hashedCentroids) {
				LOGGER.error("Missing expected hashed centroid: " + l);
			}
			if (expectedResults.count != totalResults) {
				TestUtils.deleteAll(getDataStorePluginOptions());
			}
			Assert.assertEquals(
					expectedResults.count,
					totalResults);

			final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();
			long statisticsResult = 0;
			try (CloseableIterator<InternalDataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
				while (adapterIt.hasNext()) {
					final QueryOptions queryOptions = (index == null) ? new QueryOptions() : new QueryOptions(
							index);
					final InternalDataAdapter<?> internalDataAdapter = adapterIt.next();
					queryOptions.setAggregation(
							new CountAggregation(),
							internalDataAdapter.getAdapter());
					queryOptions.setAdapter(internalDataAdapter.getAdapter());
					try (final CloseableIterator<?> countResult = geowaveStore.query(
							queryOptions,
							query)) {
						// results should already be aggregated, there should be
						// exactly one value in this iterator
						Assert.assertTrue(countResult.hasNext());
						final Object result = countResult.next();
						Assert.assertTrue(result instanceof CountResult);
						statisticsResult += ((CountResult) result).getCount();
						Assert.assertFalse(countResult.hasNext());
					}
				}
			}

			Assert.assertEquals(
					expectedResults.count,
					statisticsResult);
		}
	}

	protected void testDeleteDataId(
			final URL savedFilterResource,
			final PrimaryIndex index )
			throws Exception {
		LOGGER.warn("deleting by data ID from " + index.getId().getString() + " index");

		boolean success = false;
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = getDataStorePluginOptions().createDataStore();
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);
		final CloseableIterator<?> actualResults;

		// Run the spatial query
		actualResults = geowaveStore.query(
				new QueryOptions(
						index),
				query);

		// Grab the first one
		SimpleFeature testFeature = null;
		if (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if ((testFeature == null) && (obj instanceof SimpleFeature)) {
				testFeature = (SimpleFeature) obj;
			}
		}
		actualResults.close();

		// Delete it by data ID
		if (testFeature != null) {
			final ByteArrayId dataId = new ByteArrayId(
					testFeature.getID());
			final ByteArrayId adapterId = new ByteArrayId(
					testFeature.getFeatureType().getTypeName());

			if (geowaveStore.delete(
					new QueryOptions(
							adapterId,
							index.getId()),
					new DataIdQuery(
							dataId))) {

				success = !hasAtLeastOne(geowaveStore.query(
						new QueryOptions(
								adapterId,
								index.getId()),
						new DataIdQuery(
								dataId)));
			}
		}
		Assert.assertTrue(
				"Unable to delete entry by data ID and adapter ID",
				success);
	}

	protected void testDeleteSpatial(
			final URL savedFilterResource,
			final PrimaryIndex index )
			throws Exception {
		LOGGER.warn("bulk deleting via spatial query from " + index.getId() + " index");

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = getDataStorePluginOptions().createDataStore();

		// Run the query for this delete to get the expected count
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);

		deleteInternal(
				geowaveStore,
				index,
				query);
	}

	protected void testDeleteCQL(
			final String cqlStr,
			final PrimaryIndex index )
			throws Exception {
		LOGGER.warn("bulk deleting from " + index.getId() + " index using CQL: '" + cqlStr + "'");

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = getDataStorePluginOptions().createDataStore();

		// Retrieve the feature adapter for the CQL query generator
		PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

		try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				GeotoolsFeatureDataAdapter adapter = (GeotoolsFeatureDataAdapter) it.next().getAdapter();

				// Create the CQL query
				final Query query = CQLQuery.createOptimalQuery(
						cqlStr,
						adapter,
						null,
						null);

				deleteInternal(
						geowaveStore,
						index,
						query);
			}
		}
	}

	protected void deleteInternal(
			final mil.nga.giat.geowave.core.store.DataStore geowaveStore,
			final PrimaryIndex index,
			final Query query )
			throws IOException {
		// Query everything
		CloseableIterator<?> queryResults = geowaveStore.query(
				new QueryOptions(
						index),
				null);

		int allFeatures = 0;
		while (queryResults.hasNext()) {
			final Object obj = queryResults.next();
			if (obj instanceof SimpleFeature) {
				allFeatures++;
			}
		}
		queryResults.close();

		LOGGER.warn("Total count in table before delete: " + allFeatures);

		// Run the query for this delete to get the expected count
		queryResults = geowaveStore.query(
				new QueryOptions(
						index),
				query);

		int expectedFeaturesToDelete = 0;
		while (queryResults.hasNext()) {
			final Object obj = queryResults.next();
			if (obj instanceof SimpleFeature) {
				expectedFeaturesToDelete++;
			}
		}
		queryResults.close();

		LOGGER.warn(expectedFeaturesToDelete + " features to delete...");

		// Do the delete
		final boolean deleteResults = geowaveStore.delete(
				new QueryOptions(
						index),
				query);

		LOGGER.warn("Bulk delete results: " + (deleteResults ? "Success" : "Failure"));

		// Query again - should be zero remaining
		queryResults = geowaveStore.query(
				new QueryOptions(
						index),
				query);

		final int initialQueryFeatures = expectedFeaturesToDelete;
		int remainingFeatures = 0;
		while (queryResults.hasNext()) {
			final Object obj = queryResults.next();
			if (obj instanceof SimpleFeature) {
				remainingFeatures++;
			}
		}
		queryResults.close();

		final int deletedFeatures = initialQueryFeatures - remainingFeatures;

		LOGGER.warn(deletedFeatures + " features bulk deleted.");
		LOGGER.warn(remainingFeatures + " features not deleted.");

		// Now for the final check, query everything again
		queryResults = geowaveStore.query(
				new QueryOptions(
						index),
				null);

		int finalFeatures = 0;
		while (queryResults.hasNext()) {
			final Object obj = queryResults.next();
			if (obj instanceof SimpleFeature) {
				finalFeatures++;
			}
		}
		queryResults.close();

		LOGGER.warn("Total count in table after delete: " + finalFeatures);
		LOGGER.warn("<before> - <after> = " + (allFeatures - finalFeatures));

		Assert.assertTrue(
				"Unable to delete all features in bulk delete",
				(allFeatures - finalFeatures) == deletedFeatures);
	}

	private static boolean hasAtLeastOne(
			final CloseableIterator<?> it ) {
		try {
			return it.hasNext();
		}
		finally {
			try {
				it.close();
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected void testStats(
			final URL[] inputFiles,
			final PrimaryIndex index,
			final boolean multithreaded ) {
		testStats(
				inputFiles,
				index,
				multithreaded,
				null);
	}

	protected void testStats(
			final URL[] inputFiles,
			final PrimaryIndex index,
			final boolean multithreaded,
			final CoordinateReferenceSystem crs ) {
		// In the multithreaded case, only test min/max and count. Stats will be
		// ingested/ in a different order and will not match.
		final LocalFileIngestPlugin<SimpleFeature> localFileIngest = new GeoToolsVectorDataStoreIngestPlugin(
				Filter.INCLUDE);
		final Map<ByteArrayId, StatisticsCache> statsCache = new HashMap<ByteArrayId, StatisticsCache>();
		final Collection<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(index.getId());
		InternalAdapterStore internalAdapterStore = getDataStorePluginOptions().createInternalAdapterStore();
		final MathTransform mathTransform = TestUtils.transformFromCrs(crs);
		for (final URL inputFile : inputFiles) {
			LOGGER.warn("Calculating stats from file '" + inputFile.getPath() + "' - this may take several minutes...");
			try (final CloseableIterator<GeoWaveData<SimpleFeature>> dataIterator = localFileIngest.toGeoWaveData(
					inputFile,
					indexIds,
					null)) {
				final TransientAdapterStore adapterCache = new MemoryAdapterStore(
						localFileIngest.getDataAdapters(null));
				while (dataIterator.hasNext()) {
					final GeoWaveData<SimpleFeature> data = dataIterator.next();
					final boolean needsInit = adapterCache.adapterExists(data.getAdapterId());
					final WritableDataAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
					if (!needsInit) {
						adapter.init(index);
						adapterCache.addAdapter(adapter);
					}
					// it should be a statistical data adapter
					if (adapter instanceof StatisticsProvider) {
						StatisticsCache cachedValues = statsCache.get(adapter.getAdapterId());
						if (cachedValues == null) {
							cachedValues = new StatisticsCache(
									(StatisticsProvider<SimpleFeature>) adapter,
									internalAdapterStore.getInternalAdapterId(adapter.getAdapterId()));
							statsCache.put(
									adapter.getAdapterId(),
									cachedValues);
						}
						cachedValues.entryIngested(mathTransform != null ? FeatureDataUtils.crsTransform(
								data.getValue(),
								SimpleFeatureTypeBuilder.retype(
										data.getValue().getFeatureType(),
										crs),
								mathTransform) : data.getValue());
					}
				}
			}
			catch (final IOException e) {
				e.printStackTrace();
				TestUtils.deleteAll(getDataStorePluginOptions());
				Assert.fail("Error occurred while reading data from file '" + inputFile.getPath() + "': '"
						+ e.getLocalizedMessage() + "'");
			}
		}
		final DataStatisticsStore statsStore = getDataStorePluginOptions().createDataStatisticsStore();
		final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();
		try (CloseableIterator<InternalDataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final InternalDataAdapter<?> internalDataAdapter = adapterIterator.next();
				final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();
				final StatisticsCache cachedValue = statsCache.get(adapter.getAdapterId());
				Assert.assertNotNull(cachedValue);
				final Collection<DataStatistics<SimpleFeature>> expectedStats = cachedValue.statsCache.values();
				try (CloseableIterator<DataStatistics<?>> statsIterator = statsStore
						.getDataStatistics(internalDataAdapter.getInternalAdapterId())) {
					int statsCount = 0;
					while (statsIterator.hasNext()) {
						final DataStatistics<?> nextStats = statsIterator.next();
						if ((nextStats instanceof RowRangeHistogramStatistics)
								|| (nextStats instanceof IndexMetaDataSet)
								|| (nextStats instanceof DifferingFieldVisibilityEntryCount)
								|| (nextStats instanceof DuplicateEntryCount)
								|| (nextStats instanceof PartitionStatistics)) {
							continue;
						}
						statsCount++;
					}
					Assert.assertEquals(
							"The number of stats for data adapter '" + adapter.getAdapterId().getString()
									+ "' do not match count expected",
							expectedStats.size(),
							statsCount);
				}
				for (final DataStatistics<SimpleFeature> expectedStat : expectedStats) {
					final DataStatistics<?> actualStats = statsStore.getDataStatistics(
							internalDataAdapter.getInternalAdapterId(),
							expectedStat.getStatisticsId());

					// Only test RANGE and COUNT in the multithreaded case. None
					// of the other statistics will match!
					if (multithreaded) {
						if (!(expectedStat.getStatisticsId().getString().startsWith(
								FeatureNumericRangeStatistics.STATS_TYPE.getString() + "#")
								|| expectedStat.getStatisticsId().equals(
										CountDataStatistics.STATS_TYPE) || expectedStat
								.getStatisticsId()
								.getString()
								.startsWith(
										"FEATURE_BBOX"))) {
							continue;
						}
					}

					Assert.assertNotNull(actualStats);
					// if the stats are the same, their binary serialization
					// should be the same
					Assert.assertArrayEquals(
							actualStats.toString() + " = " + expectedStat.toString(),
							expectedStat.toBinary(),
							actualStats.toBinary());
				}
				// finally check the one stat that is more manually calculated -
				// the bounding box
				final BoundingBoxDataStatistics<?> bboxStat = (BoundingBoxDataStatistics<SimpleFeature>) statsStore
						.getDataStatistics(
								internalDataAdapter.getInternalAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getFeatureType()
										.getGeometryDescriptor()
										.getLocalName()));

				Assert.assertNotNull(bboxStat);
				Assert.assertEquals(
						"The min X of the bounding box stat does not match the expected value",
						cachedValue.minX,
						bboxStat.getMinX(),
						MathUtils.EPSILON);
				Assert.assertEquals(
						"The min Y of the bounding box stat does not match the expected value",
						cachedValue.minY,
						bboxStat.getMinY(),
						MathUtils.EPSILON);
				Assert.assertEquals(
						"The max X of the bounding box stat does not match the expected value",
						cachedValue.maxX,
						bboxStat.getMaxX(),
						MathUtils.EPSILON);
				Assert.assertEquals(
						"The max Y of the bounding box stat does not match the expected value",
						cachedValue.maxY,
						bboxStat.getMaxY(),
						MathUtils.EPSILON);
			}
		}
		catch (final IOException e) {
			e.printStackTrace();

			TestUtils.deleteAll(getDataStorePluginOptions());
			Assert.fail("Error occurred while retrieving adapters or statistics from metadata table: '"
					+ e.getLocalizedMessage() + "'");
		}
	}

	protected static class StatisticsCache implements
			IngestCallback<SimpleFeature>
	{
		// assume a bounding box statistic exists and calculate the value
		// separately to ensure calculation works
		private double minX = Double.MAX_VALUE;
		private double minY = Double.MAX_VALUE;;
		private double maxX = -Double.MAX_VALUE;;
		private double maxY = -Double.MAX_VALUE;;
		protected final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsCache = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();

		// otherwise use the statistics interface to calculate every statistic
		// and compare results to what is available in the statistics data store
		private StatisticsCache(
				final StatisticsProvider<SimpleFeature> dataAdapter,
				short internalAdapterId ) {
			final ByteArrayId[] statsIds = dataAdapter.getSupportedStatisticsTypes();
			for (final ByteArrayId statsId : statsIds) {
				final DataStatistics<SimpleFeature> stats = dataAdapter.createDataStatistics(statsId);
				stats.setInternalDataAdapterId(internalAdapterId);
				statsCache.put(
						statsId,
						stats);
			}
		}

		@Override
		public void entryIngested(
				final SimpleFeature entry,
				final GeoWaveRow... geowaveRows ) {
			for (final DataStatistics<SimpleFeature> stats : statsCache.values()) {
				stats.entryIngested(
						entry,
						geowaveRows);
			}
			final Geometry geometry = ((Geometry) entry.getDefaultGeometry());
			if ((geometry != null) && !geometry.isEmpty()) {
				minX = Math.min(
						minX,
						geometry.getEnvelopeInternal().getMinX());
				minY = Math.min(
						minY,
						geometry.getEnvelopeInternal().getMinY());
				maxX = Math.max(
						maxX,
						geometry.getEnvelopeInternal().getMaxX());
				maxY = Math.max(
						maxY,
						geometry.getEnvelopeInternal().getMaxY());
			}
		}
	}
}
