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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.util.MathUtils;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.query.cql.CQLQuery;
import org.locationtech.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.CountResult;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.DistributableQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

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
						AbstractGeoWaveBasicVectorIT.class.getClassLoader().getResource(
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
			final Index index,
			final String queryDescription )
			throws Exception {
		testQuery(
				savedFilterResource,
				expectedResultsResources,
				index,
				queryDescription,
				null,
				false);
	}

	protected void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final Index index,
			final String queryDescription,
			final CoordinateReferenceSystem crs,
			final boolean countDuplicates )
			throws Exception {
		LOGGER.info("querying " + queryDescription);
		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();
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
			final List<Long> actualCentroids = new ArrayList<>();
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
			int duplicates = 0;
			try (CloseableIterator<InternalDataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
				while (adapterIt.hasNext()) {
					final QueryOptions queryOptions = (index == null) ? new QueryOptions() : new QueryOptions(
							index);
					final InternalDataAdapter<?> internalDataAdapter = adapterIt.next();
					if (countDuplicates) {
						queryOptions.setAggregation(
								new DuplicateCountAggregation(),
								internalDataAdapter);
						queryOptions.setAdapter(internalDataAdapter);
						try (final CloseableIterator<?> countResult = geowaveStore.query(
								queryOptions,
								query)) {
							if (countResult.hasNext()) {
								final Object result = countResult.next();
								duplicates += ((CountResult) result).getCount();
							}
						}
					}
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
					statisticsResult - duplicates);
		}
	}

	public static class DuplicateCountAggregation extends
			CountAggregation
	{
		private final Set<ByteArrayId> visitedDataIds = new HashSet<>();

		@Override
		public void aggregate(
				final CommonIndexedPersistenceEncoding entry ) {
			if (!entry.isDuplicated()) {
				return;
			}
			if (visitedDataIds.contains(entry.getDataId())) {
				// only aggregate when you find a duplicate entry
				super.aggregate(entry);
			}
			visitedDataIds.add(entry.getDataId());
		}

		@Override
		public void clearResult() {
			super.clearResult();
			visitedDataIds.clear();
		}

		@Override
		public CountResult getResult() {
			final CountResult res = super.getResult();
			if (res == null) {
				// return non-null because the visited Data IDs may generate
				// duplicates on merge
				return new DuplicateCount(
						0,
						visitedDataIds);
			}

			return new DuplicateCount(
					res.getCount(),
					visitedDataIds);
		}

	}

	public static class DuplicateCount extends
			CountResult
	{
		private Set<ByteArrayId> visitedDataIds = new HashSet<>();

		public DuplicateCount() {
			super();
		}

		public DuplicateCount(
				final long value,
				final Set<ByteArrayId> visitedDataIds ) {
			super(
					value);
			this.visitedDataIds = visitedDataIds;
		}

		@Override
		public byte[] toBinary() {
			int bufferSize = 12;
			for (final ByteArrayId visited : visitedDataIds) {
				bufferSize += 4;
				bufferSize += visited.getBytes().length;
			}
			final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
			buffer.putLong(count);
			buffer.putInt(visitedDataIds.size());

			for (final ByteArrayId visited : visitedDataIds) {
				buffer.putInt(visited.getBytes().length);
				buffer.put(visited.getBytes());
			}
			return buffer.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buffer = ByteBuffer.wrap(bytes);
			count = buffer.getLong();
			final int size = buffer.getInt();
			visitedDataIds = new HashSet<>(
					size);
			for (int i = 0; i < size; i++) {
				final byte[] dataId = new byte[buffer.getInt()];
				buffer.get(dataId);
				visitedDataIds.add(new ByteArrayId(
						dataId));
			}
		}

		@Override
		public void merge(
				final Mergeable result ) {
			if (result instanceof DuplicateCount) {
				int dupes = 0;
				for (final ByteArrayId d : visitedDataIds) {
					if (((DuplicateCount) result).visitedDataIds.contains(d)) {
						dupes++;
					}
				}
				visitedDataIds.addAll(((DuplicateCount) result).visitedDataIds);
				count += ((DuplicateCount) result).count;
				// this is very important, it covers counting duplicates across
				// regions, which is the inadequacy of the aggregation in the
				// first place when there are duplicates
				count += dupes;
			}
		}

	}

	protected void testDeleteDataId(
			final URL savedFilterResource,
			final Index index )
			throws Exception {
		LOGGER.warn("deleting by data ID from " + index.getId().getString() + " index");

		boolean success = false;
		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();
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
			final Index index )
			throws Exception {
		LOGGER.warn("bulk deleting via spatial query from " + index.getId() + " index");

		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();

		// Run the query for this delete to get the expected count
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);

		deleteInternal(
				geowaveStore,
				index,
				query);
	}

	protected void testDeleteCQL(
			final String cqlStr,
			final Index index )
			throws Exception {
		LOGGER.warn("bulk deleting from " + index.getId() + " index using CQL: '" + cqlStr + "'");

		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();

		// Retrieve the feature adapter for the CQL query generator
		final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

		try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final GeotoolsFeatureDataAdapter adapter = (GeotoolsFeatureDataAdapter) it.next().getAdapter();

				// Create the CQL query
				final QueryConstraints query = CQLQuery.createOptimalQuery(
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
			final org.locationtech.geowave.core.store.api.DataStore geowaveStore,
			final Index index,
			final QueryConstraints query )
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
			final Index index,
			final boolean multithreaded ) {
		testStats(
				inputFiles,
				index,
				multithreaded,
				null);
	}

	protected void testStats(
			final URL[] inputFiles,
			final Index index,
			final boolean multithreaded,
			final CoordinateReferenceSystem crs ) {
		// In the multithreaded case, only test min/max and count. Stats will be
		// ingested/ in a different order and will not match.
		final LocalFileIngestPlugin<SimpleFeature> localFileIngest = new GeoToolsVectorDataStoreIngestPlugin(
				Filter.INCLUDE);
		final Map<ByteArrayId, StatisticsCache> statsCache = new HashMap<>();
		final Collection<ByteArrayId> indexIds = new ArrayList<>();
		indexIds.add(index.getId());
		final InternalAdapterStore internalAdapterStore = getDataStorePluginOptions().createInternalAdapterStore();
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
					final DataTypeAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
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
								|| (nextStats instanceof FieldVisibilityCount)
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
							expectedStat.getStatisticsType());

					// Only test RANGE and COUNT in the multithreaded case. None
					// of the other statistics will match!
					if (multithreaded) {
						if (!(expectedStat.getStatisticsType().getString().startsWith(
								FeatureNumericRangeStatistics.STATS_TYPE.getString() + "#")
								|| expectedStat.getStatisticsType().equals(
										CountDataStatistics.STATS_TYPE) || expectedStat
								.getStatisticsType()
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
				Assert.assertTrue(
						"Unable to remove individual stat",
						statsStore.removeStatistics(
								internalDataAdapter.getInternalAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getFeatureType()
										.getGeometryDescriptor()
										.getLocalName())));

				Assert.assertNull(
						"Individual stat was not successfully removed",
						statsStore.getDataStatistics(
								internalDataAdapter.getInternalAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getFeatureType()
										.getGeometryDescriptor()
										.getLocalName())));
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
		protected final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsCache = new HashMap<>();

		// otherwise use the statistics interface to calculate every statistic
		// and compare results to what is available in the statistics data store
		private StatisticsCache(
				final StatisticsProvider<SimpleFeature> dataAdapter,
				final short internalAdapterId ) {
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
