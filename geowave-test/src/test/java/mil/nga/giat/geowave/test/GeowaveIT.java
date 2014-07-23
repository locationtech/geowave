package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.Query;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.store.query.SpatialTemporalQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class GeowaveIT
{
	private final static Logger LOGGER = Logger.getLogger(GeowaveIT.class);
	private static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	private static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "test-cases.zip";
	private static final String TEST_CASE_BASE = "data/";
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
	private static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
	private static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
	private static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test_GeoWaveIT";

	private static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	private static AccumuloOperations accumuloOperations;
	private static String zookeeper;
	private static String accumuloInstance;
	private static String accumuloUser;
	private static String accumuloPassword;
	private static MiniAccumuloCluster miniAccumulo;
	private static File tempDir;

	@Test
	public void testIngestAndQuerySpatialPointsAndLines() {
		final Index spatialIndex = IndexType.SPATIAL.createDefaultIndex();
		// ingest both lines and points
		testIngest(
				IndexType.SPATIAL,
				HAIL_SHAPEFILE_FILE);
		testIngest(
				IndexType.SPATIAL,
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
			accumuloOperations.deleteAll();
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
			accumuloOperations.deleteAll();
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage() + "'");
		}

		try {
			testDelete(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL);
		}
		catch (final Exception e) {
			e.printStackTrace();
			accumuloOperations.deleteAll();
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '" + e.getLocalizedMessage() + "'");
		}
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines() {
		// ingest both lines and points
		testIngest(
				IndexType.SPATIAL_TEMPORAL,
				HAIL_SHAPEFILE_FILE);
		testIngest(
				IndexType.SPATIAL_TEMPORAL,
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
			accumuloOperations.deleteAll();
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
			accumuloOperations.deleteAll();
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}

		try {
			testDelete(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL_TEMPORAL);
		}
		catch (final Exception e) {
			e.printStackTrace();
			accumuloOperations.deleteAll();
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}
	}

	@BeforeClass
	public static void setup() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		unZipFile(
				GeowaveIT.class.getClassLoader().getResourceAsStream(
						TEST_DATA_ZIP_RESOURCE_PATH),
				TEST_CASE_BASE);

		zookeeper = System.getProperty("zookeeperUrl");
		accumuloInstance = System.getProperty("instance");
		accumuloUser = System.getProperty("username");
		accumuloPassword = System.getProperty("password");
		if (!isSet(zookeeper) || !isSet(accumuloInstance) || !isSet(accumuloUser) || !isSet(accumuloPassword)) {
			try {
				tempDir = Files.createTempDir();
				tempDir.deleteOnExit();
				final MiniAccumuloConfig config = new MiniAccumuloConfig(
						tempDir,
						DEFAULT_MINI_ACCUMULO_PASSWORD);
				config.setNumTservers(4);
				miniAccumulo = new MiniAccumuloCluster(
						config);
				miniAccumulo.start();
				zookeeper = miniAccumulo.getZooKeepers();
				accumuloInstance = miniAccumulo.getInstanceName();
				accumuloUser = "root";
				accumuloPassword = DEFAULT_MINI_ACCUMULO_PASSWORD;
			}
			catch (IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to start mini accumulo instance",
						e);
				LOGGER.info("Check '" + tempDir.getAbsolutePath() + File.separator + "logs' for more info");
				if (SystemUtils.IS_OS_WINDOWS) {
					LOGGER.warn("For windows, make sure that Cygwin is installed and set a CYGPATH environment variable to %CYGWIN_HOME%/bin/cygpath to successfully run a mini accumulo cluster");
				}
				Assert.fail("Unable to start mini accumulo instance: '" + e.getLocalizedMessage() + "'");
			}
		}
		try {
			accumuloOperations = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					TEST_NAMESPACE);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to connect to Accumulo",
					e);
			Assert.fail("Could not connect to Accumulo instance: '" + e.getLocalizedMessage() + "'");
		}
	}

	@AfterClass
	public static void cleanup() {
		Assert.assertTrue(
				"Index not deleted successfully",
				accumuloOperations.deleteAll());
		if (miniAccumulo != null) {
			try {
				miniAccumulo.stop();
			}
			catch (IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to stop mini accumulo instance",
						e);
			}
			try {
				// sleep because mini accumulo processes still have a hold on
				// the log files and there is no hook to get notified when it is
				// completely stopped
				Thread.sleep(1000);
				FileUtils.deleteDirectory(tempDir);
			}
			catch (final IOException | InterruptedException e) {
				LOGGER.warn(
						"Unable to delete mini Accumulo temporary directory",
						e);
			}
		}
	}

	private void testIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -t geotools -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -index " + (indexType.equals(IndexType.SPATIAL) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
	}

	private static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
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
		final mil.nga.giat.geowave.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				accumuloOperations);
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		map.put(
				"url",
				savedFilterResource);
		final SimpleFeature savedFilter;
		SimpleFeatureIterator sfi = null;
		try {
			dataStore = DataStoreFinder.getDataStore(map);

			// just grab the first feature and use it as a filter
			sfi = dataStore.getFeatureSource(
					dataStore.getNames().get(
							0)).getFeatures().features();
			savedFilter = sfi.next();

		}
		finally {
			if (sfi != null) {
				sfi.close();
			}
			dataStore.dispose();

		}
		// this file is the filtered dataset (using the previous file as a
		// filter) so use it to ensure the query worked
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		for (final URL expectedResultsResource : expectedResultsResources) {
			map.put(
					"url",
					expectedResultsResource);
			SimpleFeatureIterator featureIterator = null;
			try {
				dataStore = DataStoreFinder.getDataStore(map);
				final SimpleFeatureCollection expectedResults = dataStore.getFeatureSource(
						dataStore.getNames().get(
								0)).getFeatures();

				expectedResultCount += expectedResults.size();
				// unwrap the expected results into a set of features IDs so its
				// easy to
				// check against
				featureIterator = expectedResults.features();
				while (featureIterator.hasNext()) {
					hashedCentroids.add(hashCentroid((Geometry) featureIterator.next().getDefaultGeometry()));
				}
			}
			finally {
				featureIterator.close();
				dataStore.dispose();
			}
		}
		final CloseableIterator<?> actualResults;
		if (index == null) {
			actualResults = geowaveStore.query(savedFilterToQuery(savedFilter));
		}
		else {
			actualResults = geowaveStore.query(
					index,
					savedFilterToQuery(savedFilter));
		}
		int totalResults = 0;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if (obj instanceof SimpleFeature) {
				final SimpleFeature result = (SimpleFeature) obj;
				Assert.assertTrue(
						"Actual result '" + result.toString() + "' not found in expected result set",
						hashedCentroids.contains(hashCentroid((Geometry) result.getDefaultGeometry())));
				totalResults++;
			}
			else {
				accumuloOperations.deleteAll();
				Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
			}
		}
		if (expectedResultCount != totalResults) {
			accumuloOperations.deleteAll();
		}
		Assert.assertEquals(
				expectedResultCount,
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
		final mil.nga.giat.geowave.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				accumuloOperations);
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		map.put(
				"url",
				savedFilterResource);
		final SimpleFeature savedFilter;
		SimpleFeatureIterator sfi = null;
		try {
			dataStore = DataStoreFinder.getDataStore(map);

			// just grab the first feature and use it as a filter
			sfi = dataStore.getFeatureSource(
					dataStore.getNames().get(
							0)).getFeatures().features();
			savedFilter = sfi.next();

		}
		finally {
			if (sfi != null) {
				sfi.close();
			}
			dataStore.dispose();
		}

		final Index index = indexType.createDefaultIndex();
		final CloseableIterator<?> actualResults;

		actualResults = geowaveStore.query(
				index,
				savedFilterToQuery(savedFilter));

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

	private long hashCentroid(
			final Geometry geometry ) {
		final Point centroid = geometry.getCentroid();
		return Double.doubleToLongBits(centroid.getX()) + Double.doubleToLongBits(centroid.getY() * 31);
	}

	private Query savedFilterToQuery(
			final SimpleFeature savedFilter ) {
		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TEST_FILTER_END_TIME_ATTRIBUTE_NAME);

		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			Date startDate = null, endDate = null;
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (startObj instanceof Date) {
				endDate = (Date) endObj;
			}
			if ((startDate != null) && (endDate != null)) {
				return new SpatialTemporalQuery(
						startDate,
						endDate,
						filterGeometry);
			}
		}
		// otherwise just return a spatial query
		return new SpatialQuery(
				filterGeometry);
	}

	/**
	 * Unzips the contents of a zip input stream to a target output directory if
	 * the file exists and is the same size as the zip entry, it is not
	 * overwritten
	 * 
	 * @param zipFile
	 *            input zip file
	 * @param output
	 *            zip file output folder
	 */
	private static void unZipFile(
			final InputStream zipStream,
			final String outputFolder ) {

		final byte[] buffer = new byte[1024];

		try {

			// create output directory is not exists
			final File folder = new File(
					outputFolder);
			if (!folder.exists()) {
				folder.mkdir();
			}

			// get the zip file content
			final ZipInputStream zis = new ZipInputStream(
					zipStream);
			// get the zipped file list entry
			ZipEntry ze = zis.getNextEntry();

			while (ze != null) {
				if (ze.isDirectory()) {
					ze = zis.getNextEntry();
					continue;
				}
				final String fileName = ze.getName();
				final File newFile = new File(
						outputFolder + File.separator + fileName);
				if (newFile.exists()) {
					if (newFile.length() == ze.getSize()) {
						ze = zis.getNextEntry();
						continue;
					}
					else {
						newFile.delete();
					}
				}

				// create all non exists folders
				new File(
						newFile.getParent()).mkdirs();

				final FileOutputStream fos = new FileOutputStream(
						newFile);

				int len;
				while ((len = zis.read(buffer)) > 0) {
					fos.write(
							buffer,
							0,
							len);
				}

				fos.close();
				ze = zis.getNextEntry();
			}

			zis.closeEntry();
			zis.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to extract test data",
					e);
			Assert.fail("Unable to extract test data: '" + e.getLocalizedMessage() + "'");
		}
	}
}
