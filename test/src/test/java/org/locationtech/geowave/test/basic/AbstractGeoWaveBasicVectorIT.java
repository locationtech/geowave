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
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import edu.emory.mathcs.backport.java.util.Arrays;

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
		final QueryConstraints constraints = TestUtils.resourceToQuery(savedFilterResource);
		QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();
		if (index != null) {
			bldr = bldr.indexName(index.getName());
		}
		try (final CloseableIterator<?> actualResults = geowaveStore.query(bldr.constraints(
				constraints).build())) {
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
					AggregationQueryBuilder<?, Long, ?, ?> aggBldr = AggregationQueryBuilder.newBuilder();
					if (index != null) {
						aggBldr = aggBldr.indexName(index.getName());
					}
					aggBldr = aggBldr.constraints(constraints);
					final InternalDataAdapter<?> internalDataAdapter = adapterIt.next();
					if (countDuplicates) {
						aggBldr.aggregate(
								internalDataAdapter.getTypeName(),
								(Aggregation) new DuplicateCountAggregation());
						final DuplicateCount countResult = (DuplicateCount) geowaveStore
								.aggregate((AggregationQuery) aggBldr.build());
						if (countResult != null) {
							duplicates += countResult.count;
						}
					}
					aggBldr.count(internalDataAdapter.getTypeName());
					final Long countResult = geowaveStore.aggregate(aggBldr.build());
					// results should already be aggregated, there should be
					// exactly one value in this iterator
					Assert.assertTrue(countResult != null);
					statisticsResult += countResult;
				}
			}

			Assert.assertEquals(
					expectedResults.count,
					statisticsResult - duplicates);
		}
	}

	public static class DuplicateCountAggregation implements
			CommonIndexAggregation<Persistable, DuplicateCount>
	{
		private final Set<ByteArray> visitedDataIds = new HashSet<>();
		long count = 0;

		@Override
		public void aggregate(
				final CommonIndexedPersistenceEncoding entry ) {
			if (!entry.isDuplicated()) {
				return;
			}
			if (visitedDataIds.contains(entry.getDataId())) {
				// only aggregate when you find a duplicate entry
				count++;
			}
			visitedDataIds.add(entry.getDataId());
		}

		@Override
		public void clearResult() {
			count = 0;
			visitedDataIds.clear();
		}

		@Override
		public Persistable getParameters() {
			return null;
		}

		@Override
		public void setParameters(
				final Persistable parameters ) {}

		@Override
		public DuplicateCount merge(
				final DuplicateCount result1,
				final DuplicateCount result2 ) {
			int dupes = 0;
			for (final ByteArray d : result1.visitedDataIds) {
				if (result2.visitedDataIds.contains(d)) {
					dupes++;
				}
			}
			result1.visitedDataIds.addAll(result2.visitedDataIds);
			result1.count += result2.count;
			// this is very important, it covers counting duplicates across
			// regions, which is the inadequacy of the aggregation in the
			// first place when there are duplicates
			result1.count += dupes;
			return result1;
		}

		@Override
		public DuplicateCount getResult() {
			return new DuplicateCount(
					count,
					visitedDataIds);
		}

		@Override
		public byte[] resultToBinary(
				final DuplicateCount result ) {
			int bufferSize = 12;
			for (final ByteArray visited : visitedDataIds) {
				bufferSize += 4;
				bufferSize += visited.getBytes().length;
			}
			final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
			buffer.putLong(count);
			buffer.putInt(visitedDataIds.size());

			for (final ByteArray visited : visitedDataIds) {
				buffer.putInt(visited.getBytes().length);
				buffer.put(visited.getBytes());
			}
			return buffer.array();
		}

		@Override
		public DuplicateCount resultFromBinary(
				final byte[] binary ) {
			final ByteBuffer buffer = ByteBuffer.wrap(binary);
			final long count = buffer.getLong();
			final int size = buffer.getInt();
			final Set<ByteArray> visitedDataIds = new HashSet<>(
					size);
			for (int i = 0; i < size; i++) {
				final byte[] dataId = new byte[buffer.getInt()];
				buffer.get(dataId);
				visitedDataIds.add(new ByteArray(
						dataId));
			}
			return new DuplicateCount(
					count,
					visitedDataIds);
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

	}

	public static class DuplicateCount
	{
		private long count;
		private Set<ByteArray> visitedDataIds = new HashSet<>();

		public DuplicateCount() {
			super();
		}

		public DuplicateCount(
				final long count,
				final Set<ByteArray> visitedDataIds ) {
			this.count = count;
			this.visitedDataIds = visitedDataIds;
		}

	}

	protected void testDeleteDataId(
			final URL savedFilterResource,
			final Index index )
			throws Exception {
		LOGGER.warn("deleting by data ID from " + index.getName() + " index");

		boolean success = false;
		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();
		final QueryConstraints query = TestUtils.resourceToQuery(savedFilterResource);
		final CloseableIterator<?> actualResults;

		// Run the spatial query
		actualResults = geowaveStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				query).build());

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
			final ByteArray dataId = new ByteArray(
					testFeature.getID());

			if (geowaveStore.delete(QueryBuilder.newBuilder().addTypeName(
					testFeature.getFeatureType().getTypeName()).indexName(
					index.getName()).constraints(
					new DataIdQuery(
							dataId)).build())) {

				success = !hasAtLeastOne(geowaveStore.query(QueryBuilder.newBuilder().addTypeName(
						testFeature.getFeatureType().getTypeName()).indexName(
						index.getName()).constraints(
						new DataIdQuery(
								dataId)).build()));
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
		LOGGER.warn("bulk deleting via spatial query from " + index.getName() + " index");

		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();

		// Run the query for this delete to get the expected count
		final QueryConstraints query = TestUtils.resourceToQuery(savedFilterResource);

		deleteInternal(
				geowaveStore,
				index,
				query);
	}

	protected void testDeleteCQL(
			final String cqlStr,
			final Index index )
			throws Exception {
		LOGGER.warn("bulk deleting from " + index.getName() + " index using CQL: '" + cqlStr + "'");

		final org.locationtech.geowave.core.store.api.DataStore geowaveStore = getDataStorePluginOptions()
				.createDataStore();

		// Retrieve the feature adapter for the CQL query generator
		final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

		try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final GeotoolsFeatureDataAdapter adapter = (GeotoolsFeatureDataAdapter) it.next().getAdapter();

				// Create the CQL query
				final QueryConstraints query = OptimalCQLQuery.createOptimalQuery(
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
		CloseableIterator<?> queryResults = geowaveStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).build());

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
		queryResults = geowaveStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				query).build());
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
		final boolean deleteResults = geowaveStore.delete(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				query).build());

		LOGGER.warn("Bulk delete results: " + (deleteResults ? "Success" : "Failure"));

		// Query again - should be zero remaining
		queryResults = geowaveStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				query).build());

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

		Assert.assertTrue(
				"Unable to delete all features in bulk delete, there are " + remainingFeatures + " not deleted",
				remainingFeatures == 0);
		// Now for the final check, query everything again
		queryResults = geowaveStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).build());

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
			it.close();
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
		final Map<String, StatisticsCache> statsCache = new HashMap<>();
		final String[] indexNames = new String[] {
			index.getName()
		};
		final InternalAdapterStore internalAdapterStore = getDataStorePluginOptions().createInternalAdapterStore();
		final MathTransform mathTransform = TestUtils.transformFromCrs(crs);
		for (final URL inputFile : inputFiles) {
			LOGGER.warn("Calculating stats from file '" + inputFile.getPath() + "' - this may take several minutes...");
			try (final CloseableIterator<GeoWaveData<SimpleFeature>> dataIterator = localFileIngest.toGeoWaveData(
					inputFile,
					indexNames,
					null)) {
				final TransientAdapterStore adapterCache = new MemoryAdapterStore(
						localFileIngest.getDataAdapters(null));
				while (dataIterator.hasNext()) {
					final GeoWaveData<SimpleFeature> data = dataIterator.next();
					final boolean needsInit = adapterCache.adapterExists(data.getTypeName());
					final DataTypeAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
					if (!needsInit && (adapter instanceof InitializeWithIndicesDataAdapter)) {
						((InitializeWithIndicesDataAdapter) adapter).init(index);
						adapterCache.addAdapter(adapter);
					}
					// it should be a statistical data adapter
					if (adapter instanceof StatisticsProvider) {
						StatisticsCache cachedValues = statsCache.get(adapter.getTypeName());
						if (cachedValues == null) {
							cachedValues = new StatisticsCache(
									(StatisticsProvider<SimpleFeature>) adapter,
									internalAdapterStore.getAdapterId(adapter.getTypeName()));
							statsCache.put(
									adapter.getTypeName(),
									cachedValues);
						}
						cachedValues.entryIngested(mathTransform != null ? GeometryUtils.crsTransform(
								data.getValue(),
								SimpleFeatureTypeBuilder.retype(
										data.getValue().getFeatureType(),
										crs),
								mathTransform) : data.getValue());
					}
				}
			}
		}
		final DataStatisticsStore statsStore = getDataStorePluginOptions().createDataStatisticsStore();
		final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();
		try (CloseableIterator<InternalDataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final InternalDataAdapter<?> internalDataAdapter = adapterIterator.next();
				final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();
				final StatisticsCache cachedValue = statsCache.get(adapter.getTypeName());
				Assert.assertNotNull(cachedValue);
				final Collection<InternalDataStatistics<SimpleFeature, ?, ?>> expectedStats = cachedValue.statsCache
						.values();
				try (CloseableIterator<InternalDataStatistics<?, ?, ?>> statsIterator = statsStore
						.getDataStatistics(internalDataAdapter.getAdapterId())) {
					int statsCount = 0;
					while (statsIterator.hasNext()) {
						final InternalDataStatistics<?, ?, ?> nextStats = statsIterator.next();
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
							"The number of stats for data adapter '" + adapter.getTypeName()
									+ "' do not match count expected",
							expectedStats.size(),
							statsCount);
				}
				for (final InternalDataStatistics<SimpleFeature, ?, ?> expectedStat : expectedStats) {
					try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> actualStatsIt = statsStore
							.getDataStatistics(
									internalDataAdapter.getAdapterId(),
									expectedStat.getExtendedId(),
									expectedStat.getType())) {
						if (actualStatsIt.hasNext()) {
							final InternalDataStatistics<?, ?, ?> actualStats = actualStatsIt.next();

							// Only test RANGE and COUNT in the multithreaded
							// case. None
							// of the other statistics will match!
							if (multithreaded) {
								if (!(expectedStat.getType().getString().startsWith(
										FeatureNumericRangeStatistics.STATS_TYPE.getString())
										|| expectedStat.getType().equals(
												CountDataStatistics.STATS_TYPE) || expectedStat
										.getType()
										.getString()
										.startsWith(
												"BOUNDING_BOX"))) {
									continue;
								}
							}

							Assert.assertNotNull(actualStats);
							// if the stats are the same, their binary
							// serialization should be the same
							Assert.assertArrayEquals(
									actualStats.toString() + " = " + expectedStat.toString(),
									expectedStat.toBinary(),
									actualStats.toBinary());

						}

					}
				}
				// finally check the one stat that is more manually calculated -
				// the bounding box
				final StatisticsQuery<Envelope> query = VectorStatisticsQueryBuilder
						.newBuilder()
						.factory()
						.bbox()
						.fieldName(
								adapter.getFeatureType().getGeometryDescriptor().getLocalName())
						.dataType(
								adapter.getTypeName())
						.build();
				final StatisticsId id = query.getId();
				final Envelope bboxStat = getDataStorePluginOptions().createDataStore().aggregateStatistics(
						query);
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
								internalDataAdapter.getAdapterId(),
								id.getExtendedId(),
								id.getType()));

				try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> statsIt = statsStore.getDataStatistics(
						internalDataAdapter.getAdapterId(),
						id.getExtendedId(),
						id.getType())) {
					Assert.assertFalse(
							"Individual stat was not successfully removed",
							statsIt.hasNext());
				}
			}
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
		protected final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsCache = new HashMap<>();

		// otherwise use the statistics interface to calculate every statistic
		// and compare results to what is available in the statistics data store
		private StatisticsCache(
				final StatisticsProvider<SimpleFeature> dataAdapter,
				final short internalAdapterId ) {
			final StatisticsId[] statsIds = dataAdapter.getSupportedStatistics();
			for (final StatisticsId statsId : statsIds) {
				final InternalDataStatistics<SimpleFeature, ?, ?> stats = dataAdapter.createDataStatistics(statsId);
				stats.setAdapterId(internalAdapterId);
				statsCache.put(
						statsId,
						stats);
			}
		}

		@Override
		public void entryIngested(
				final SimpleFeature entry,
				final GeoWaveRow... geowaveRows ) {
			for (final InternalDataStatistics<SimpleFeature, ?, ?> stats : statsCache.values()) {
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
