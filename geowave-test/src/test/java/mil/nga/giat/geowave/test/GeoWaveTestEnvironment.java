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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.store.query.SpatialTemporalQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

abstract public class GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveTestEnvironment.class);
	protected static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
	protected static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
	protected static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	protected static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	protected static final String TEST_CASE_BASE = "data/";
	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	protected static final Object MUTEX = new Object();
	protected static AccumuloOperations accumuloOperations;
	protected static String zookeeper;
	protected static String accumuloInstance;
	protected static String accumuloUser;
	protected static String accumuloPassword;
	protected static MiniAccumuloCluster miniAccumulo;
	protected static File tempDir;

	protected static boolean DEFER_CLEANUP = false;

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	@BeforeClass
	public static void setup() {
		synchronized (MUTEX) {
			TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
			if (accumuloOperations == null) {
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
						config.setNumTservers(2);
						miniAccumulo = new MiniAccumuloCluster(
								config);
						if (SystemUtils.IS_OS_WINDOWS && isYarn()) {
							// this must happen after instantiating Mini
							// Accumulo Cluster because it ensures the accumulo
							// directory is empty or it will fail, but must
							// happen before the cluster is started because yarn
							// expects winutils.exe to exist within a bin
							// directory in the mini accumulo cluster directory
							// (mini accumulo cluster will always set this
							// directory as hadoop_home)
							LOGGER.info("Running YARN on windows requires a local installation of Hadoop");
							LOGGER.info("'HADOOP_HOME' must be set and 'PATH' must contain %HADOOP_HOME%/bin");

							final Map<String, String> env = System.getenv();
							String hadoopHome = System.getProperty("hadoop.home.dir");
							if (hadoopHome == null) {
								hadoopHome = env.get("HADOOP_HOME");
							}
							boolean success = false;
							if (hadoopHome != null) {
								final File hadoopDir = new File(
										hadoopHome);
								if (hadoopDir.exists()) {
									final File binDir = new File(
											tempDir,
											"bin");
									binDir.mkdir();
									FileUtils.copyFile(
											new File(
													hadoopDir + File.separator + "bin",
													HADOOP_WINDOWS_UTIL),
											new File(
													binDir,
													HADOOP_WINDOWS_UTIL));
									success = true;
								}
							}
							if (!success) {
								LOGGER.error("'HADOOP_HOME' environment variable is not set or <HADOOP_HOME>/bin/winutils.exe does not exist");
								return;
							}
						}

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
		}
	}

	protected static boolean isSet(
			final String str ) {
		return (str != null) && !str.isEmpty();
	}

	@AfterClass
	public static void cleanup() {
		synchronized (MUTEX) {
			if (!DEFER_CLEANUP) {
				Assert.assertTrue(
						"Index not deleted successfully",
						(accumuloOperations == null) || accumuloOperations.deleteAll());
				accumuloOperations = null;
				zookeeper = null;
				accumuloInstance = null;
				accumuloUser = null;
				accumuloPassword = null;
				if (miniAccumulo != null) {
					try {
						miniAccumulo.stop();
						miniAccumulo = null;
					}
					catch (IOException | InterruptedException e) {
						LOGGER.warn(
								"Unable to stop mini accumulo instance",
								e);
					}
				}
//				if (tempDir != null) {
//					try {
//						// sleep because mini accumulo processes still have a
//						// hold
//						// on the log files and there is no hook to get notified
//						// when it is completely stopped
//						Thread.sleep(1000);
//						FileUtils.deleteDirectory(tempDir);
//						tempDir = null;
//					}
//					catch (final IOException | InterruptedException e) {
//						LOGGER.warn(
//								"Unable to delete mini Accumulo temporary directory",
//								e);
//					}
//				}
			}
		}
	}

	public static void addAuthorization(
			final String auth,
			final BasicAccumuloOperations accumuloOperations ) {
		try {
			accumuloOperations.insureAuthorization(auth);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to alter authorization for Accumulo user",
					e);
			Assert.fail("Unable to alter authorization for Accumulo user: '" + e.getLocalizedMessage() + "'");
		}
	}

	protected static long hashCentroid(
			final Geometry geometry ) {
		final Point centroid = geometry.getCentroid();
		return Double.doubleToLongBits(centroid.getX()) + Double.doubleToLongBits(centroid.getY() * 31);
	}

	protected static class ExpectedResults
	{
		public Set<Long> hashedCentroids;
		public int count;

		protected ExpectedResults(
				final Set<Long> hashedCentroids,
				final int count ) {
			this.hashedCentroids = hashedCentroids;
			this.count = count;
		}
	}

	protected static ExpectedResults getExpectedResults(
			final CloseableIterator<?> results )
			throws IOException {
		final Set<Long> hashedCentroids = new HashSet<Long>();
		int expectedResultCount = 0;
		try {
			while (results.hasNext()) {
				final Object obj = results.next();
				if (obj instanceof SimpleFeature) {
					expectedResultCount++;
					final SimpleFeature feature = (SimpleFeature) obj;
					hashedCentroids.add(hashCentroid((Geometry) feature.getDefaultGeometry()));
				}
			}
		}
		finally {
			results.close();
		}
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	protected static ExpectedResults getExpectedResults(
			final URL[] expectedResultsResources )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
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
				// easy to check against
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
		return new ExpectedResults(
				hashedCentroids,
				expectedResultCount);
	}

	protected static DistributableQuery resourceToQuery(
			final URL filterResource )
			throws IOException {
		return featureToQuery(resourceToFeature(filterResource));
	}

	protected static SimpleFeature resourceToFeature(
			final URL filterResource )
			throws IOException {
		final Map<String, Object> map = new HashMap<String, Object>();
		DataStore dataStore = null;
		map.put(
				"url",
				filterResource);
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
		return savedFilter;
	}

	protected static DistributableQuery featureToQuery(
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

	public static void addAuthorization(
			final String auth ) {
		try {
			((BasicAccumuloOperations) accumuloOperations).insureAuthorization(auth);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to alter authorization for Accumulo user",
					e);
			Assert.fail("Unable to alter authorization for Accumulo user: '" + e.getLocalizedMessage() + "'");
		}
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
	protected static void unZipFile(
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

	static protected void replaceParameters(
			final Map<String, String> values,
			final File file )
			throws IOException {
		{
			String str = FileUtils.readFileToString(file);
			for (final Entry<String, String> entry : values.entrySet()) {
				str = str.replaceAll(
						entry.getKey(),
						entry.getValue());
			}
			FileUtils.deleteQuietly(file);
			FileUtils.write(
					file,
					str);
		}
	}
}
