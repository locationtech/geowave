package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.util.MathUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class GeoWaveBasicIT extends
		GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveBasicIT.class);
	private static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
	private static final String TEST_FILTER_PACKAGE = TEST_CASE_BASE + "filter/";
	private static final String HAIL_TEST_CASE_PACKAGE = TEST_CASE_BASE + "hail_test_case/";
	private static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-polygon-filter.shp";
	private static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-temporal-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-polygon-temporal-filter.shp";
	private static final String TORNADO_TRACKS_TEST_CASE_PACKAGE = TEST_CASE_BASE + "tornado_tracks_test_case/";
	private static final String TORNADO_TRACKS_SHAPEFILE_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-temporal-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String TEST_BOX_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
	private static final String TEST_POLYGON_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						GeoWaveBasicIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

	@Test
	public void testIngestAndQuerySpatialPointsAndLines() {
		final Index spatialIndex = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		// ingest both lines and points
		testLocalIngest(
				IndexType.SPATIAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testLocalIngest(
				IndexType.SPATIAL_VECTOR,
				TORNADO_TRACKS_SHAPEFILE_FILE);

		try {
			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
					},
					spatialIndex,
					"bounding box constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}

			Assert.fail("Error occurred while testing a bounding box query of spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			testQuery(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
					},
					spatialIndex,
					"polygon constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			testStats(
					new File[] {
						new File(
								HAIL_SHAPEFILE_FILE),
						new File(
								TORNADO_TRACKS_SHAPEFILE_FILE)
					},
					spatialIndex);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing a bounding box stats on spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			testDelete(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL_VECTOR);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
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
				final StatisticalDataAdapter<SimpleFeature> dataAdapter ) {
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

	public void testStats(
			final File[] inputFiles,
			final Index index ) {
		final LocalFileIngestPlugin<SimpleFeature> localFileIngest = new GeoToolsVectorDataStoreIngestPlugin(
				Filter.INCLUDE);
		final Map<ByteArrayId, StatisticsCache> statsCache = new HashMap<ByteArrayId, StatisticsCache>();
		for (final File inputFile : inputFiles) {
			LOGGER.warn("Calculating stats from file '" + inputFile.getName() + "' - this may take several minutes...");
			try (final CloseableIterator<GeoWaveData<SimpleFeature>> dataIterator = localFileIngest.toGeoWaveData(
					inputFile,
					index.getId(),
					null)) {
				final AdapterStore adapterCache = new MemoryAdapterStore(
						localFileIngest.getDataAdapters(null));
				while (dataIterator.hasNext()) {
					final GeoWaveData<SimpleFeature> data = dataIterator.next();
					final WritableDataAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
					// it should be a statistical data adapter
					if (adapter instanceof StatisticalDataAdapter) {
						StatisticsCache cachedValues = statsCache.get(adapter.getAdapterId());
						if (cachedValues == null) {
							cachedValues = new StatisticsCache(
									(StatisticalDataAdapter<SimpleFeature>) adapter);
							statsCache.put(
									adapter.getAdapterId(),
									cachedValues);
						}
						final DataStoreEntryInfo entryInfo = AccumuloUtils.getIngestInfo(
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
				try {
					accumuloOperations.deleteAll();
				}
				catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
					LOGGER.error(
							"Unable to clear accumulo namespace",
							ex);
				}
				Assert.fail("Error occurred while reading data from file '" + inputFile.getAbsolutePath() + "': '" + e.getLocalizedMessage() + "'");
			}
		}
		final DataStatisticsStore statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);
		final AdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);
		try (CloseableIterator<DataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final FeatureDataAdapter adapter = (FeatureDataAdapter) adapterIterator.next();
				final StatisticsCache cachedValue = statsCache.get(adapter.getAdapterId());
				Assert.assertNotNull(cachedValue);
				final Collection<DataStatistics<SimpleFeature>> expectedStats = cachedValue.statsCache.values();
				try (CloseableIterator<DataStatistics<?>> statsIterator = statsStore.getDataStatistics(adapter.getAdapterId())) {
					int statsCount = 0;
					while (statsIterator.hasNext()) {
						statsIterator.next();
						statsCount++;
					}
					Assert.assertEquals(
							"The number of stats for data adapter '" + adapter.getAdapterId().getString() + "' do not match count expected",
							expectedStats.size(),
							statsCount);
				}

				for (final DataStatistics<SimpleFeature> expectedStat : expectedStats) {
					final DataStatistics<?> actualStats = statsStore.getDataStatistics(
							expectedStat.getDataAdapterId(),
							expectedStat.getStatisticsId());
					Assert.assertNotNull(actualStats);
					// if the stats are the same, their binary serialization
					// should be the same
					Assert.assertArrayEquals(
							expectedStat.toBinary(),
							actualStats.toBinary());
				}
				// finally check the one stat that is more manually calculated -
				// the bounding box
				final BoundingBoxDataStatistics<?> bboxStat = (BoundingBoxDataStatistics<SimpleFeature>) statsStore.getDataStatistics(
						adapter.getAdapterId(),
						FeatureBoundingBoxStatistics.composeId(adapter.getType().getGeometryDescriptor().getLocalName()));

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
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while retrieving adapters or statistics from metadata table: '" + e.getLocalizedMessage() + "'");
		}
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines() {
		final Index spatialTemporalIndex = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		// ingest both lines and points
		testLocalIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testLocalIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				TORNADO_TRACKS_SHAPEFILE_FILE);
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
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing a bounding box and time range query of spatial temporal index: '" + e.getLocalizedMessage() + "'");
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
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}

		try {
			testStats(
					new File[] {
						new File(
								HAIL_SHAPEFILE_FILE),
						new File(
								TORNADO_TRACKS_SHAPEFILE_FILE)
					},
					spatialTemporalIndex);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing a bounding box stats on spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			testDelete(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL_TEMPORAL_VECTOR);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
			}
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
		}
	}

	@Test
	public void testFeatureSerialization() {

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
				"This is my string. There are many like it, but this one is mine.\n" + "My string is my best friend. It is my life. I must master it as I must master my life.");
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
		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		for (final Map.Entry<Class, Object> arg : args.entrySet()) {
			serBuilder.set(
					arg.getKey().getName(),
					arg.getValue());
		}

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);

		final SimpleFeature sf = serBuilder.buildFeature("343");
		geowaveStore.ingest(
				serAdapter,
				index,
				sf);
		final DistributableQuery q = new SpatialQuery(
				((Geometry) args.get(Geometry.class)).buffer(0.5d));
		final CloseableIterator<?> iter = geowaveStore.query(q);
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
					Assert.assertArrayEquals(
							(double[]) before,
							(double[]) after,
							1e-12d);
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
					Assert.assertArrayEquals(
							(float[]) before,
							(float[]) after,
							1e-12f);
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
		IOUtils.closeQuietly(iter);
		Assert.assertTrue(
				"One feature should be found",
				foundFeat);
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error(
					"Unable to clear accumulo namespace",
					ex);
		}
	}

	public <T> T[] returnArray(
			final Class<T> clazz,
			final Object o ) {
		return (T[]) o;
	}

	private void testIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -f geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		GeoWaveMain.main(args);
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
			final Index index,
			final String queryDescription )
			throws Exception {
		LOGGER.info("querying " + queryDescription);
		System.out.println("querying " + queryDescription);
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
		// this file is the filtered dataset (using the previous file as a
		// filter) so use it to ensure the query worked
		final DistributableQuery query = resourceToQuery(savedFilterResource);
		final CloseableIterator<?> actualResults;
		if (index == null) {
			actualResults = geowaveStore.query(query);
		}
		else {
			actualResults = geowaveStore.query(
					index,
					query);
		}
		final ExpectedResults expectedResults = getExpectedResults(expectedResultsResources);
		int totalResults = 0;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if (obj instanceof SimpleFeature) {
				final SimpleFeature result = (SimpleFeature) obj;
				Assert.assertTrue(
						"Actual result '" + result.toString() + "' not found in expected result set",
						expectedResults.hashedCentroids.contains(hashCentroid((Geometry) result.getDefaultGeometry())));
				totalResults++;
			}
			else {
				try {
					accumuloOperations.deleteAll();
				}
				catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
					LOGGER.error(
							"Unable to clear accumulo namespace",
							ex);
				}
				Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
			}
		}
		if (expectedResults.count != totalResults) {
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error(
						"Unable to clear accumulo namespace",
						ex);
				Assert.fail("Unable to clear accumulo namespace");
			}
		}
		Assert.assertEquals(
				expectedResults.count,
				totalResults);
		actualResults.close();
	}

	private void testDelete(
			final URL savedFilterResource,
			final IndexType indexType )
			throws Exception {
		LOGGER.info("deleting from " + indexType.toString() + " index");
		System.out.println("deleting from " + indexType.toString() + " index");
		boolean success = false;
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
		final DistributableQuery query = resourceToQuery(savedFilterResource);
		final Index index = indexType.createDefaultIndex();
		final CloseableIterator<?> actualResults;

		actualResults = geowaveStore.query(
				index,
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

			if (geowaveStore.deleteEntry(
					index,
					dataId,
					adapterId)) {

				if (geowaveStore.getEntry(
						index,
						dataId,
						adapterId) == null) {
					success = true;
				}
			}
		}
		Assert.assertTrue(
				"Unable to delete entry by data ID and adapter ID",
				success);
	}

}
