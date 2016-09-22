package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.util.MathUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportOptions;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.TestUtils.ExpectedResults;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicIT
{
	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss'Z'");
	private static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
	private static final String TEST_FILTER_PACKAGE = TestUtils.TEST_CASE_BASE + "filter/";
	private static final String HAIL_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE + "hail_test_case/";
	private static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";
	private static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-box-temporal-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-temporal-filter.shp";
	private static final String TORNADO_TRACKS_TEST_CASE_PACKAGE = TestUtils.TEST_CASE_BASE
			+ "tornado_tracks_test_case/";
	private static final String TORNADO_TRACKS_SHAPEFILE_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-temporal-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String TEST_BOX_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
	private static final String TEST_POLYGON_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";
	private static final String TEST_EXPORT_DIRECTORY = "export";
	private static final String TEST_BASE_EXPORT_FILE_NAME = "basicIT-export.avro";

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private final static Logger LOGGER = Logger.getLogger(GeoWaveBasicIT.class);
	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		TestUtils.unZipFile(
				new File(
						GeoWaveBasicIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveBasicIT        *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveBasicIT          *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testMultiThreadedIngestAndQuerySpatialPointsAndLines() {
		testIngestAndQuerySpatialPointsAndLines(4);
	}

	@Test
	public void testSingleThreadedIngestAndQuerySpatialPointsAndLines() {
		testIngestAndQuerySpatialPointsAndLines(1);
	}

	public void testIngestAndQuerySpatialPointsAndLines(
			final int nthreads ) {
		long mark = System.currentTimeMillis();

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				nthreads);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				nthreads);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + nthreads + " thread(s).");

		try {
			mark = System.currentTimeMillis();

			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
					},
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"bounding box constraint only");

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("BBOX query duration = " + dur + " ms.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}
		try {
			mark = System.currentTimeMillis();

			testQuery(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
					},
					TestUtils.DEFAULT_SPATIAL_INDEX,
					"polygon constraint only");

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("POLY query duration = " + dur + " ms.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage()
					+ "'");
		}

		if ((nthreads > 0)) {
			try {
				testStats(
						new File[] {
							new File(
									HAIL_SHAPEFILE_FILE),
							new File(
									TORNADO_TRACKS_SHAPEFILE_FILE)
						},
						TestUtils.DEFAULT_SPATIAL_INDEX,
						true);
			}
			catch (final Exception e) {
				e.printStackTrace();
				TestUtils.deleteAll(dataStore);
				Assert.fail("Error occurred while testing a bounding box stats on spatial index: '"
						+ e.getLocalizedMessage() + "'");
			}
		}
		try {
			testDelete(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					TestUtils.DEFAULT_SPATIAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
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
				final StatisticsProvider<SimpleFeature> dataAdapter ) {
			final ByteArrayId[] statsIds = dataAdapter.getSupportedStatisticsIds();
			for (final ByteArrayId statsId : statsIds) {
				final DataStatistics<SimpleFeature> stats = dataAdapter.createDataStatistics(statsId);
				statsCache.put(
						statsId,
						stats);
			}
		}

		@Override
		public void entryIngested(
				final DataStoreEntryInfo entryInfo,
				final SimpleFeature entry ) {
			for (final DataStatistics<SimpleFeature> stats : statsCache.values()) {
				stats.entryIngested(
						entryInfo,
						entry);
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

	private void testStats(
			final File[] inputFiles,
			final PrimaryIndex index,
			final boolean multithreaded ) {
		// In the multithreaded case, only test min/max and count. Stats will be
		// ingested
		// in a different order and will not match.
		final LocalFileIngestPlugin<SimpleFeature> localFileIngest = new GeoToolsVectorDataStoreIngestPlugin(
				Filter.INCLUDE);
		final Map<ByteArrayId, StatisticsCache> statsCache = new HashMap<ByteArrayId, StatisticsCache>();
		final Collection<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(index.getId());
		for (final File inputFile : inputFiles) {
			LOGGER.warn("Calculating stats from file '" + inputFile.getName() + "' - this may take several minutes...");
			try (final CloseableIterator<GeoWaveData<SimpleFeature>> dataIterator = localFileIngest.toGeoWaveData(
					inputFile,
					indexIds,
					null)) {
				final AdapterStore adapterCache = new MemoryAdapterStore(
						localFileIngest.getDataAdapters(null));
				while (dataIterator.hasNext()) {
					final GeoWaveData<SimpleFeature> data = dataIterator.next();
					final WritableDataAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
					// it should be a statistical data adapter
					if (adapter instanceof StatisticsProvider) {
						StatisticsCache cachedValues = statsCache.get(adapter.getAdapterId());
						if (cachedValues == null) {
							cachedValues = new StatisticsCache(
									(StatisticsProvider<SimpleFeature>) adapter);
							statsCache.put(
									adapter.getAdapterId(),
									cachedValues);
						}
						final DataStoreEntryInfo entryInfo = DataStoreUtils.getIngestInfo(
								adapter,
								index,
								data.getValue(),
								new UniformVisibilityWriter<SimpleFeature>(
										new GlobalVisibilityHandler<SimpleFeature, Object>(
												"")));
						cachedValues.entryIngested(
								entryInfo,
								data.getValue());
					}
				}
			}
			catch (final IOException e) {
				e.printStackTrace();
				TestUtils.deleteAll(dataStore);
				Assert.fail("Error occurred while reading data from file '" + inputFile.getAbsolutePath() + "': '"
						+ e.getLocalizedMessage() + "'");
			}
		}
		final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
		final AdapterStore adapterStore = dataStore.createAdapterStore();
		try (CloseableIterator<DataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final FeatureDataAdapter adapter = (FeatureDataAdapter) adapterIterator.next();
				final StatisticsCache cachedValue = statsCache.get(adapter.getAdapterId());
				Assert.assertNotNull(cachedValue);
				final Collection<DataStatistics<SimpleFeature>> expectedStats = cachedValue.statsCache.values();
				try (CloseableIterator<DataStatistics<?>> statsIterator = statsStore.getDataStatistics(adapter
						.getAdapterId())) {
					int statsCount = 0;
					while (statsIterator.hasNext()) {
						final DataStatistics<?> nextStats = statsIterator.next();
						if ((nextStats instanceof RowRangeHistogramStatistics)
								|| (nextStats instanceof IndexMetaDataSet)
								|| (nextStats instanceof DifferingFieldVisibilityEntryCount)
								|| (nextStats instanceof DuplicateEntryCount)) {
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
							expectedStat.getDataAdapterId(),
							expectedStat.getStatisticsId());

					// Only test RANGE and COUNT in the multithreaded case. None
					// of the other
					// statistics will match!
					if (multithreaded) {
						if (!(expectedStat.getStatisticsId().getString().startsWith(
								FeatureNumericRangeStatistics.STATS_TYPE + "#") || expectedStat
								.getStatisticsId()
								.equals(
										CountDataStatistics.STATS_ID))) {
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
								adapter.getAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getType()
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

			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while retrieving adapters or statistics from metadata table: '"
					+ e.getLocalizedMessage() + "'");
		}
	}

	private void testSpatialTemporalLocalExportAndReingestWithCQL(
			final URL filterURL )
			throws CQLException,
			IOException {

		final SimpleFeature savedFilter = TestUtils.resourceToFeature(filterURL);

		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_END_TIME_ATTRIBUTE_NAME);
		Date startDate = null, endDate = null;
		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (endObj instanceof Date) {
				endDate = (Date) endObj;
			}
		}
		final AdapterStore adapterStore = dataStore.createAdapterStore();
		final VectorLocalExportCommand exportCommand = new VectorLocalExportCommand();
		final VectorLocalExportOptions options = exportCommand.getOptions();
		final File exportDir = new File(
				TestUtils.TEMP_DIR,
				TEST_EXPORT_DIRECTORY);
		exportDir.delete();
		exportDir.mkdirs();

		exportCommand.setInputStoreOptions(dataStore);
		options.setBatchSize(10000);
		final Envelope env = filterGeometry.getEnvelopeInternal();
		final double east = env.getMaxX();
		final double west = env.getMinX();
		final double south = env.getMinY();
		final double north = env.getMaxY();
		try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
			while (adapterIt.hasNext()) {
				final DataAdapter<?> adapter = adapterIt.next();
				final List<String> adapterIds = new ArrayList<String>();
				adapterIds.add(adapter.getAdapterId().getString());
				options.setAdapterIds(adapterIds);
				if (adapter instanceof GeotoolsFeatureDataAdapter) {
					final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
					final TimeDescriptors timeDesc = gtAdapter.getTimeDescriptors();

					String startTimeAttribute;
					if (timeDesc.getStartRange() != null) {
						startTimeAttribute = timeDesc.getStartRange().getLocalName();
					}
					else {
						startTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String endTimeAttribute;
					if (timeDesc.getEndRange() != null) {
						endTimeAttribute = timeDesc.getEndRange().getLocalName();
					}
					else {
						endTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String geometryAttribute = gtAdapter.getType().getGeometryDescriptor().getLocalName();

					final String cqlPredicate = String.format(
							"BBOX(\"%s\",%f,%f,%f,%f) AND \"%s\" <= '%s' AND \"%s\" >= '%s'",
							geometryAttribute,
							west,
							south,
							east,
							north,
							startTimeAttribute,
							CQL_DATE_FORMAT.format(endDate),
							endTimeAttribute,
							CQL_DATE_FORMAT.format(startDate));
					options.setOutputFile(new File(
							exportDir,
							adapter.getAdapterId().getString() + TEST_BASE_EXPORT_FILE_NAME));
					options.setCqlFilter(cqlPredicate);
					exportCommand.setParameters(null);
					exportCommand.execute(new ManualOperationParams());
				}
			}
		}
		TestUtils.deleteAll(dataStore);
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				exportDir.getAbsolutePath(),
				"avro",
				4);
		try {
			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"reingested bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert
					.fail("Error occurred on reingested dataset while testing a bounding box and time range query of spatial temporal index: '"
							+ e.getLocalizedMessage() + "'");
		}
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines() {
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				HAIL_SHAPEFILE_FILE,
				1);

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		try {
			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testQuery(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"polygon constraint and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testStats(
					new File[] {
						new File(
								HAIL_SHAPEFILE_FILE),
						new File(
								TORNADO_TRACKS_SHAPEFILE_FILE)
					},
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX,
					false);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box stats on spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testSpatialTemporalLocalExportAndReingestWithCQL(new File(
					TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL());
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDelete(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
	}

	@Test
	public void testFeatureSerialization()
			throws IOException {

		final Map<Class, Object> args = new HashMap<>();
		args.put(
				Geometry.class,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								123.4,
								567.8)).buffer(
						1));
		args.put(
				Integer.class,
				23);
		args.put(
				Long.class,
				473874387l);
		args.put(
				Boolean.class,
				Boolean.TRUE);
		args.put(
				Byte.class,
				(byte) 0xa);
		args.put(
				Short.class,
				Short.valueOf("2"));
		args.put(
				Float.class,
				34.23434f);
		args.put(
				Double.class,
				85.3498394839d);
		args.put(
				byte[].class,
				new byte[] {
					(byte) 1,
					(byte) 2,
					(byte) 3
				});
		args.put(
				Byte[].class,
				new Byte[] {
					(byte) 4,
					(byte) 5,
					(byte) 6
				});
		args.put(
				Date.class,
				new Date(
						8675309l));
		args.put(
				BigInteger.class,
				BigInteger.valueOf(893489348343423l));
		args.put(
				BigDecimal.class,
				new BigDecimal(
						"939384.93840238409237483617837483"));
		args.put(
				Calendar.class,
				Calendar.getInstance());
		args.put(
				String.class,
				"This is my string. There are many like it, but this one is mine.\n"
						+ "My string is my best friend. It is my life. I must master it as I must master my life.");
		args.put(
				long[].class,
				new long[] {
					12345l,
					6789l,
					1011l,
					1213111111111111l
				});
		args.put(
				int[].class,
				new int[] {
					-55,
					-44,
					-33,
					-934839,
					55
				});
		args.put(
				double[].class,
				new double[] {
					1.125d,
					2.25d
				});
		args.put(
				float[].class,
				new float[] {
					1.5f,
					1.75f
				});
		args.put(
				short[].class,
				new short[] {
					(short) 8,
					(short) 9,
					(short) 10
				});

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();
		builder.setName("featureserializationtest");

		for (final Map.Entry<Class, Object> arg : args.entrySet()) {
			builder.add(ab.binding(
					arg.getKey()).nillable(
					false).buildDescriptor(
					arg.getKey().getName().toString()));
		}

		final SimpleFeatureType serTestType = builder.buildFeatureType();
		final SimpleFeatureBuilder serBuilder = new SimpleFeatureBuilder(
				serTestType);
		final FeatureDataAdapter serAdapter = new FeatureDataAdapter(
				serTestType);

		for (final Map.Entry<Class, Object> arg : args.entrySet()) {
			serBuilder.set(
					arg.getKey().getName(),
					arg.getValue());
		}

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStore.createDataStore();

		final SimpleFeature sf = serBuilder.buildFeature("343");
		try (IndexWriter writer = geowaveStore.createWriter(
				serAdapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			writer.write(sf);
		}
		final DistributableQuery q = new SpatialQuery(
				((Geometry) args.get(Geometry.class)).buffer(0.5d));
		try (final CloseableIterator<?> iter = geowaveStore.query(
				new QueryOptions(/* TODO do I need to pass 'index'? */),
				q)) {
			boolean foundFeat = false;
			while (iter.hasNext()) {
				final Object maybeFeat = iter.next();
				Assert.assertTrue(
						"Iterator should return simple feature in this test",
						maybeFeat instanceof SimpleFeature);
				foundFeat = true;
				final SimpleFeature isFeat = (SimpleFeature) maybeFeat;
				for (final Property p : isFeat.getProperties()) {
					final Object before = args.get(p.getType().getBinding());
					final Object after = isFeat.getAttribute(p.getType().getName().toString());

					if (before instanceof double[]) {
						Assert.assertTrue(Arrays.equals(
								(double[]) before,
								(double[]) after));
					}
					else if (before instanceof boolean[]) {
						final boolean[] b = (boolean[]) before;
						final boolean[] a = (boolean[]) after;
						Assert.assertTrue(a.length == b.length);
						for (int i = 0; i < b.length; i++) {
							Assert.assertTrue(b[i] == a[i]);
						}
					}
					else if (before instanceof byte[]) {
						Assert.assertArrayEquals(
								(byte[]) before,
								(byte[]) after);
					}
					else if (before instanceof char[]) {
						Assert.assertArrayEquals(
								(char[]) before,
								(char[]) after);
					}
					else if (before instanceof float[]) {
						Assert.assertTrue(Arrays.equals(
								(float[]) before,
								(float[]) after));
					}
					else if (before instanceof int[]) {
						Assert.assertArrayEquals(
								(int[]) before,
								(int[]) after);
					}
					else if (before instanceof long[]) {
						Assert.assertArrayEquals(
								(long[]) before,
								(long[]) after);
					}
					else if (before instanceof short[]) {
						Assert.assertArrayEquals(
								(short[]) before,
								(short[]) after);
					}
					else if (before.getClass().isArray()) {
						Assert.assertArrayEquals(
								returnArray(
										p.getType().getBinding(),
										before),
								returnArray(
										p.getType().getBinding(),
										after));
					}
					else {
						Assert.assertTrue(before.equals(after));
					}
				}
			}
			Assert.assertTrue(
					"One feature should be found",
					foundFeat);
		}

		TestUtils.deleteAll(dataStore);
	}

	public <T> T[] returnArray(
			final Class<T> clazz,
			final Object o ) {
		return (T[]) o;
	}

	private void testQuery(
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

	private void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final PrimaryIndex index,
			final String queryDescription )
			throws Exception {
		LOGGER.info("querying " + queryDescription);
		System.out.println("querying " + queryDescription);
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStore.createDataStore();
		// this file is the filtered dataset (using the previous file as a
		// filter) so use it to ensure the query worked
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);
		try (final CloseableIterator<?> actualResults = (index == null) ? geowaveStore.query(
				new QueryOptions(),
				query) : geowaveStore.query(
				new QueryOptions(
						index),
				query)) {
			final ExpectedResults expectedResults = TestUtils.getExpectedResults(expectedResultsResources);
			int totalResults = 0;
			while (actualResults.hasNext()) {
				final Object obj = actualResults.next();
				if (obj instanceof SimpleFeature) {
					final SimpleFeature result = (SimpleFeature) obj;
					final long actualHashCentroid = TestUtils.hashCentroid((Geometry) result.getDefaultGeometry());
					Assert.assertTrue(
							"Actual result '" + result.toString() + "' not found in expected result set",
							expectedResults.hashedCentroids.contains(actualHashCentroid));
					totalResults++;
				}
				else {
					TestUtils.deleteAll(dataStore);
					Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
				}
			}
			if (expectedResults.count != totalResults) {
				TestUtils.deleteAll(dataStore);
			}
			Assert.assertEquals(
					expectedResults.count,
					totalResults);

			final AdapterStore adapterStore = dataStore.createAdapterStore();
			long statisticsResult = 0;
			try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
				while (adapterIt.hasNext()) {
					final QueryOptions queryOptions = (index == null) ? new QueryOptions() : new QueryOptions(
							index);
					final DataAdapter<?> adapter = adapterIt.next();
					queryOptions.setAggregation(
							new CountAggregation(),
							adapter);
					queryOptions.setAdapter(adapter);
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

	private void testDelete(
			final URL savedFilterResource,
			final PrimaryIndex index )
			throws Exception {
		LOGGER.info("deleting from " + index.getId() + " index");
		System.out.println("deleting from " + index.getId() + " index");
		boolean success = false;
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStore.createDataStore();
		final DistributableQuery query = TestUtils.resourceToQuery(savedFilterResource);
		final CloseableIterator<?> actualResults;

		actualResults = geowaveStore.query(
				new QueryOptions(
						index),
				query);

		SimpleFeature testFeature = null;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if ((testFeature == null) && (obj instanceof SimpleFeature)) {
				testFeature = (SimpleFeature) obj;
			}
		}
		actualResults.close();

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
							adapterId,
							dataId))) {

				success = !hasAtLeastOne(geowaveStore.query(
						new QueryOptions(
								adapterId,
								index.getId()),
						new DataIdQuery(
								adapterId,
								dataId)));
			}
		}
		Assert.assertTrue(
				"Unable to delete entry by data ID and adapter ID",
				success);
	}

	private boolean hasAtLeastOne(
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
}