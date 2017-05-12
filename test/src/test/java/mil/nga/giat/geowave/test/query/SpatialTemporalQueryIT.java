package mil.nga.giat.geowave.test.query;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.index.IndexQueryStrategySPI;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginException;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions.PartitionStrategy;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class SpatialTemporalQueryIT
{
	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss'Z'");
	private static final int MULTI_DAY_YEAR = 2016;
	private static final int MULTI_DAY_MONTH = 1;
	private static final int MULTI_MONTH_YEAR = 2000;
	private static final int MULTI_YEAR_MIN = 1980;
	private static final int MULTI_YEAR_MAX = 1995;
	private static final PrimaryIndex DAY_INDEX = new SpatialTemporalIndexBuilder().setPartitionStrategy(
			PartitionStrategy.ROUND_ROBIN).setNumPartitions(
			10).setPeriodicity(
			Unit.DAY).createIndex();
	private static final PrimaryIndex MONTH_INDEX = new SpatialTemporalIndexBuilder().setPartitionStrategy(
			PartitionStrategy.HASH).setNumPartitions(
			100).setPeriodicity(
			Unit.MONTH).createIndex();
	private static final PrimaryIndex YEAR_INDEX = new SpatialTemporalIndexBuilder().setPartitionStrategy(
			PartitionStrategy.HASH).setNumPartitions(
			10).setPeriodicity(
			Unit.YEAR).createIndex();
	private FeatureDataAdapter timeStampAdapter;
	private FeatureDataAdapter timeRangeAdapter;
	private DataStore dataStore;
	private GeoWaveGTDataStore geowaveGtDataStore;
	private PrimaryIndex currentGeotoolsIndex;

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		// GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialTemporalQueryIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*     RUNNING SpatialTemporalQueryIT    *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    FINISHED SpatialTemporalQueryIT    *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Before
	public void initSpatialTemporalTestData()
			throws IOException,
			GeoWavePluginException {

		dataStore = dataStoreOptions.createDataStore();
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName("simpletimestamp");
		builder.add(
				"geo",
				Point.class);
		builder.add(
				"timestamp",
				Date.class);
		timeStampAdapter = new FeatureDataAdapter(
				builder.buildFeatureType());

		builder = new SimpleFeatureTypeBuilder();
		builder.setName("simpletimerange");
		builder.add(
				"geo",
				Point.class);
		builder.add(
				"startTime",
				Date.class);
		builder.add(
				"endTime",
				Date.class);
		timeRangeAdapter = new FeatureDataAdapter(
				builder.buildFeatureType());

		Calendar cal = getInitialDayCalendar();
		final GeometryFactory geomFactory = new GeometryFactory();
		final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
				timeStampAdapter.getFeatureType());
		final SimpleFeatureBuilder featureTimeRangeBuilder = new SimpleFeatureBuilder(
				timeRangeAdapter.getFeatureType());
		final IndexWriter timeWriters = dataStore.createWriter(
				timeStampAdapter,
				YEAR_INDEX,
				MONTH_INDEX,
				DAY_INDEX);

		final IndexWriter rangeWriters = dataStore.createWriter(
				timeRangeAdapter,
				YEAR_INDEX,
				MONTH_INDEX,
				DAY_INDEX);

		try {
			for (int day = cal.getActualMinimum(Calendar.DAY_OF_MONTH); day <= cal
					.getActualMaximum(Calendar.DAY_OF_MONTH); day++) {
				final double ptVal = ((((day + 1.0) - cal.getActualMinimum(Calendar.DAY_OF_MONTH)) / ((cal
						.getActualMaximum(Calendar.DAY_OF_MONTH) - cal.getActualMinimum(Calendar.DAY_OF_MONTH)) + 2.0)) * 2) - 1;
				cal.set(
						Calendar.DAY_OF_MONTH,
						day);
				final Point pt = geomFactory.createPoint(new Coordinate(
						ptVal,
						ptVal));
				featureBuilder.add(pt);
				featureBuilder.add(cal.getTime());
				final SimpleFeature feature = featureBuilder.buildFeature("day:" + day);
				timeWriters.write(feature);
			}

			ingestTimeRangeData(
					cal,
					rangeWriters,
					featureTimeRangeBuilder,
					cal.getActualMinimum(Calendar.DAY_OF_MONTH),
					cal.getActualMaximum(Calendar.DAY_OF_MONTH),
					Calendar.DAY_OF_MONTH,
					"day");
			cal = getInitialMonthCalendar();
			for (int month = cal.getActualMinimum(Calendar.MONTH); month <= cal.getActualMaximum(Calendar.MONTH); month++) {
				cal.set(
						Calendar.MONTH,
						month);

				final double ptVal = ((((month + 1.0) - cal.getActualMinimum(Calendar.MONTH)) / ((cal
						.getActualMaximum(Calendar.MONTH) - cal.getActualMinimum(Calendar.MONTH)) + 2.0)) * 2) - 1;
				final Point pt = geomFactory.createPoint(new Coordinate(
						ptVal,
						ptVal));
				featureBuilder.add(pt);
				featureBuilder.add(cal.getTime());
				final SimpleFeature feature = featureBuilder.buildFeature("month:" + month);
				timeWriters.write(feature);
			}
			ingestTimeRangeData(
					cal,
					rangeWriters,
					featureTimeRangeBuilder,
					cal.getActualMinimum(Calendar.MONTH),
					cal.getActualMaximum(Calendar.MONTH),
					Calendar.MONTH,
					"month");

			cal = getInitialYearCalendar();
			for (int year = MULTI_YEAR_MIN; year <= MULTI_YEAR_MAX; year++) {
				final double ptVal = ((((year + 1.0) - MULTI_YEAR_MIN) / ((MULTI_YEAR_MAX - MULTI_YEAR_MIN) + 2.0)) * 2) - 1;
				cal.set(
						Calendar.YEAR,
						year);
				final Point pt = geomFactory.createPoint(new Coordinate(
						ptVal,
						ptVal));
				featureBuilder.add(pt);
				featureBuilder.add(cal.getTime());

				final SimpleFeature feature = featureBuilder.buildFeature("year:" + year);
				timeWriters.write(feature);
			}

			ingestTimeRangeData(
					cal,
					rangeWriters,
					featureTimeRangeBuilder,
					MULTI_YEAR_MIN,
					MULTI_YEAR_MAX,
					Calendar.YEAR,
					"year");

			Point pt = geomFactory.createPoint(new Coordinate(
					-50,
					-50));
			featureBuilder.add(pt);
			featureBuilder.add(cal.getTime());
			SimpleFeature feature = featureBuilder.buildFeature("outlier1timestamp");
			timeWriters.write(feature);

			pt = geomFactory.createPoint(new Coordinate(
					50,
					50));
			featureBuilder.add(pt);
			featureBuilder.add(cal.getTime());
			feature = featureBuilder.buildFeature("outlier2timestamp");
			timeWriters.write(feature);

			pt = geomFactory.createPoint(new Coordinate(
					-50,
					-50));
			featureTimeRangeBuilder.add(pt);
			featureTimeRangeBuilder.add(cal.getTime());
			cal.roll(
					Calendar.MINUTE,
					5);
			featureTimeRangeBuilder.add(cal.getTime());
			feature = featureTimeRangeBuilder.buildFeature("outlier1timerange");
			timeWriters.write(feature);

			pt = geomFactory.createPoint(new Coordinate(
					50,
					50));
			featureTimeRangeBuilder.add(pt);
			featureTimeRangeBuilder.add(cal.getTime());
			cal.roll(
					Calendar.MINUTE,
					5);
			featureTimeRangeBuilder.add(cal.getTime());
			feature = featureTimeRangeBuilder.buildFeature("outlier2timerange");
			timeWriters.write(feature);
		}
		finally {
			timeWriters.close();
			rangeWriters.close();
		}
		geowaveGtDataStore = new GeoWaveGTDataStore(
				new GeoWavePluginConfig(
						dataStoreOptions) {
					@Override
					public IndexQueryStrategySPI getIndexQueryStrategy() {
						return new IndexQueryStrategySPI() {

							@Override
							public CloseableIterator<Index<?, ?>> getIndices(
									final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
									final BasicQuery query,
									final PrimaryIndex[] indices ) {
								final ServiceLoader<IndexQueryStrategySPI> ldr = ServiceLoader
										.load(IndexQueryStrategySPI.class);
								final Iterator<IndexQueryStrategySPI> it = ldr.iterator();
								final List<Index<?, ?>> indexList = new ArrayList<Index<?, ?>>();

								for (final PrimaryIndex index : indices) {
									indexList.add(index);
								}
								while (it.hasNext()) {
									final IndexQueryStrategySPI strategy = it.next();
									final CloseableIterator<Index<?, ?>> indexStrategyIt = strategy.getIndices(
											stats,
											query,
											indices);
									Assert.assertTrue(
											"Index Strategy '" + strategy.toString()
													+ "' must at least choose one index.",
											indexStrategyIt.hasNext());
								}
								return new CloseableIteratorWrapper<Index<?, ?>>(
										new Closeable() {

											@Override
											public void close()
													throws IOException {

											}
										},
										(Iterator) Collections.singleton(
												currentGeotoolsIndex).iterator());
							}
						};
					}
				});
	}

	@After
	public void deleteTestData()
			throws IOException {
		TestUtils.deleteAll(dataStoreOptions);
	}

	private static Calendar getInitialDayCalendar() {
		final Calendar cal = Calendar.getInstance();
		cal.set(
				MULTI_DAY_YEAR,
				MULTI_DAY_MONTH,
				1,
				1,
				1,
				1);
		cal.set(
				Calendar.MILLISECOND,
				0);
		return cal;
	}

	private static Calendar getInitialMonthCalendar() {
		final Calendar cal = Calendar.getInstance();
		cal.set(
				MULTI_MONTH_YEAR,
				1,
				1,
				1,
				1,
				1);
		cal.set(
				Calendar.MILLISECOND,
				0);

		return cal;
	}

	private static Calendar getInitialYearCalendar() {
		final Calendar cal = Calendar.getInstance();
		cal.set(
				Calendar.DAY_OF_MONTH,
				1);
		cal.set(
				Calendar.MONTH,
				1);
		cal.set(
				Calendar.HOUR_OF_DAY,
				1);
		cal.set(
				Calendar.MINUTE,
				1);
		cal.set(
				Calendar.SECOND,
				1);
		cal.set(
				Calendar.MILLISECOND,
				0);
		return cal;
	}

	private static void write(
			final IndexWriter[] writers,
			final SimpleFeature feature )
			throws IOException {
		for (final IndexWriter writer : writers) {
			writer.write(feature);
		}
	}

	private static void ingestTimeRangeData(
			final Calendar cal,
			final IndexWriter writer,
			final SimpleFeatureBuilder featureTimeRangeBuilder,
			final int min,
			final int max,
			final int field,
			final String name )
			throws IOException {
		final GeometryFactory geomFactory = new GeometryFactory();
		final int midPoint = (int) Math.floor((min + max) / 2.0);
		cal.set(
				field,
				min);
		featureTimeRangeBuilder.add(geomFactory.createPoint(new Coordinate(
				0,
				0)));
		featureTimeRangeBuilder.add(cal.getTime());
		cal.set(
				field,
				max);
		featureTimeRangeBuilder.add(cal.getTime());
		SimpleFeature feature = featureTimeRangeBuilder.buildFeature(name + ":fullrange");
		writer.write(feature);

		cal.set(
				field,
				min);
		featureTimeRangeBuilder.add(geomFactory.createPoint(new Coordinate(
				-0.1,
				-0.1)));
		featureTimeRangeBuilder.add(cal.getTime());
		cal.set(
				field,
				midPoint);
		featureTimeRangeBuilder.add(cal.getTime());
		feature = featureTimeRangeBuilder.buildFeature(name + ":firsthalfrange");
		writer.write(feature);
		featureTimeRangeBuilder.add(geomFactory.createPoint(new Coordinate(
				0.1,
				0.1)));
		featureTimeRangeBuilder.add(cal.getTime());
		cal.set(
				field,
				max);

		featureTimeRangeBuilder.add(cal.getTime());
		feature = featureTimeRangeBuilder.buildFeature(name + ":secondhalfrange");
		writer.write(feature);
	}

	private void testQueryMultipleBins(
			final Calendar cal,
			final int field,
			final int min,
			final int max,
			final QueryOptions options,
			final String name )
			throws IOException,
			CQLException {
		options.setAdapter(timeStampAdapter);
		cal.set(
				field,
				min);
		Date startOfQuery = cal.getTime();
		final int midPoint = (int) Math.floor((min + max) / 2.0);
		cal.set(
				field,
				midPoint);
		Date endOfQuery = cal.getTime();

		testQueryMultipleBinsGivenDateRange(
				options,
				name,
				min,
				midPoint,
				startOfQuery,
				endOfQuery);
		cal.set(
				field,
				midPoint);
		startOfQuery = cal.getTime();
		cal.set(
				field,
				max);
		endOfQuery = cal.getTime();

		testQueryMultipleBinsGivenDateRange(
				options,
				name,
				midPoint,
				max,
				startOfQuery,
				endOfQuery);
	}

	private void testQueryMultipleBinsGivenDateRange(
			final QueryOptions options,
			final String name,
			final int minExpectedResult,
			final int maxExpectedResult,
			final Date startOfQuery,
			final Date endOfQuery )
			throws CQLException,
			IOException {
		final Set<String> fidExpectedResults = new HashSet<String>(
				(maxExpectedResult - minExpectedResult) + 1);
		for (int i = minExpectedResult; i <= maxExpectedResult; i++) {
			fidExpectedResults.add(name + ":" + i);
		}
		testQueryGivenDateRange(
				options,
				name,
				fidExpectedResults,
				startOfQuery,
				endOfQuery,
				timeStampAdapter.getAdapterId().getString(),
				"timestamp",
				"timestamp");
	}

	private void testQueryGivenDateRange(
			final QueryOptions options,
			final String name,
			final Set<String> fidExpectedResults,
			final Date startOfQuery,
			final Date endOfQuery,
			final String adapterId,
			final String startTimeAttribute,
			final String endTimeAttribute )
			throws CQLException,
			IOException {
		final String cqlPredicate = "BBOX(\"geo\",-1,-1,1,1) AND \"" + startTimeAttribute + "\" <= '"
				+ CQL_DATE_FORMAT.format(endOfQuery) + "' AND \"" + endTimeAttribute + "\" >= '"
				+ CQL_DATE_FORMAT.format(startOfQuery) + "'";
		final Set<String> fidResults = new HashSet<String>();
		try (CloseableIterator<SimpleFeature> it = dataStore.query(
				options,
				new SpatialTemporalQuery(
						startOfQuery,
						endOfQuery,
						new GeometryFactory().toGeometry(new Envelope(
								-1,
								1,
								-1,
								1))))) {
			while (it.hasNext()) {
				final SimpleFeature feature = it.next();
				fidResults.add(feature.getID());
			}
		}
		assertFidsMatchExpectation(
				name,
				fidExpectedResults,
				fidResults);

		final Set<String> geotoolsFidResults = new HashSet<String>();
		// now make sure geotools results match
		try (final SimpleFeatureIterator features = geowaveGtDataStore.getFeatureSource(
				adapterId).getFeatures(
				CQL.toFilter(cqlPredicate)).features()) {
			while (features.hasNext()) {
				final SimpleFeature feature = features.next();
				geotoolsFidResults.add(feature.getID());
			}
		}
		assertFidsMatchExpectation(
				name,
				fidExpectedResults,
				geotoolsFidResults);
	}

	private void assertFidsMatchExpectation(
			final String name,
			final Set<String> fidExpectedResults,
			final Set<String> fidResults ) {
		Assert.assertEquals(
				"Expected result count does not match actual result count for " + name,
				fidExpectedResults.size(),
				fidResults.size());
		final Iterator<String> it = fidExpectedResults.iterator();
		while (it.hasNext()) {
			final String expectedFid = it.next();
			Assert.assertTrue(
					"Cannot find result for " + expectedFid,
					fidResults.contains(expectedFid));
		}
	}

	@Test
	public void testQueryMultipleBinsDay()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(DAY_INDEX);
		currentGeotoolsIndex = DAY_INDEX;
		final Calendar cal = getInitialDayCalendar();
		testQueryMultipleBins(
				cal,
				Calendar.DAY_OF_MONTH,
				cal.getActualMinimum(Calendar.DAY_OF_MONTH),
				cal.getActualMaximum(Calendar.DAY_OF_MONTH),
				options,
				"day");
	}

	@Test
	public void testQueryMultipleBinsMonth()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(MONTH_INDEX);
		currentGeotoolsIndex = MONTH_INDEX;
		final Calendar cal = getInitialMonthCalendar();
		testQueryMultipleBins(
				cal,
				Calendar.MONTH,
				cal.getActualMinimum(Calendar.MONTH),
				cal.getActualMaximum(Calendar.MONTH),
				options,
				"month");

	}

	@Test
	public void testQueryMultipleBinsYear()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(YEAR_INDEX);
		currentGeotoolsIndex = YEAR_INDEX;
		final Calendar cal = getInitialYearCalendar();

		testQueryMultipleBins(
				cal,
				Calendar.YEAR,
				MULTI_YEAR_MIN,
				MULTI_YEAR_MAX,
				options,
				"year");
	}

	private void testTimeRangeAcrossBins(
			final Calendar cal,
			final int field,
			final int min,
			final int max,
			final QueryOptions options,
			final String name )
			throws IOException,
			CQLException {
		cal.set(
				field,
				min);
		Date startOfQuery = cal.getTime();
		final int midPoint = (int) Math.floor((min + max) / 2.0);
		cal.set(
				field,
				midPoint - 1);
		Date endOfQuery = cal.getTime();
		Set<String> fidExpectedResults = new HashSet<String>();
		fidExpectedResults.add(name + ":fullrange");
		fidExpectedResults.add(name + ":firsthalfrange");

		testQueryGivenDateRange(
				options,
				name,
				fidExpectedResults,
				startOfQuery,
				endOfQuery,
				timeRangeAdapter.getAdapterId().getString(),
				"startTime",
				"endTime");

		cal.set(
				field,
				midPoint + 1);
		startOfQuery = cal.getTime();
		cal.set(
				field,
				max);
		endOfQuery = cal.getTime();
		fidExpectedResults = new HashSet<String>();
		fidExpectedResults.add(name + ":fullrange");
		fidExpectedResults.add(name + ":secondhalfrange");

		testQueryGivenDateRange(
				options,
				name,
				fidExpectedResults,
				startOfQuery,
				endOfQuery,
				timeRangeAdapter.getAdapterId().getString(),
				"startTime",
				"endTime");

		cal.set(
				field,
				min);
		startOfQuery = cal.getTime();
		cal.set(
				field,
				max);
		endOfQuery = cal.getTime();

		fidExpectedResults.add(name + ":fullrange");
		fidExpectedResults.add(name + ":firsthalfrange");
		fidExpectedResults.add(name + ":secondhalfrange");
		testQueryGivenDateRange(
				options,
				name,
				fidExpectedResults,
				startOfQuery,
				endOfQuery,
				timeRangeAdapter.getAdapterId().getString(),
				"startTime",
				"endTime");
	}

	@Test
	public void testTimeRangeAcrossBinsDay()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(DAY_INDEX);
		currentGeotoolsIndex = DAY_INDEX;
		options.setAdapter(timeRangeAdapter);
		final Calendar cal = getInitialDayCalendar();
		testTimeRangeAcrossBins(
				cal,
				Calendar.DAY_OF_MONTH,
				cal.getActualMinimum(Calendar.DAY_OF_MONTH),
				cal.getActualMaximum(Calendar.DAY_OF_MONTH),
				options,
				"day");
	}

	@Test
	public void testTimeRangeAcrossBinsMonth()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(MONTH_INDEX);
		currentGeotoolsIndex = MONTH_INDEX;
		options.setAdapter(timeRangeAdapter);
		final Calendar cal = getInitialMonthCalendar();
		testTimeRangeAcrossBins(
				cal,
				Calendar.MONTH,
				cal.getActualMinimum(Calendar.MONTH),
				cal.getActualMaximum(Calendar.MONTH),
				options,
				"month");
	}

	@Test
	public void testTimeRangeAcrossBinsYear()
			throws IOException,
			CQLException {
		final QueryOptions options = new QueryOptions();
		options.setIndex(YEAR_INDEX);
		currentGeotoolsIndex = YEAR_INDEX;
		options.setAdapter(timeRangeAdapter);
		final Calendar cal = getInitialYearCalendar();
		testTimeRangeAcrossBins(
				cal,
				Calendar.YEAR,
				MULTI_YEAR_MIN,
				MULTI_YEAR_MAX,
				options,
				"year");

	}
}
