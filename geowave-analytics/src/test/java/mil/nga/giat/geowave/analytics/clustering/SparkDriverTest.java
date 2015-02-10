package mil.nga.giat.geowave.analytics.clustering;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.spark.GeowaveRDD;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class SparkDriverTest
{
	private final static Logger LOGGER = Logger.getLogger(SparkDriverTest.class);
	protected static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	protected static final String HDFS_BASE_DIRECTORY = "test_tmp";
	protected static final String DEFAULT_JOB_TRACKER = "local";
	protected static final int MIN_INPUT_SPLITS = 2;
	protected static final int MAX_INPUT_SPLITS = 4;
	protected static String jobtracker;
	protected static String hdfs;
	protected static boolean hdfsProtocol;
	protected static String hdfsBaseDirectory;

	protected static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	protected static final String TEST_CASE_BASE = "data/";
	protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
	protected static final Object MUTEX = new Object();
	protected static AccumuloOperations accumuloOperations;
	protected static String zookeeper;
	protected static String accumuloInstance;
	protected static String accumuloUser;
	protected static String accumuloPassword;
	protected static File tempDir;

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	@BeforeClass
	public static void setup() throws AccumuloException, AccumuloSecurityException {

		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

		final Map<String, String> env = System.getenv();

		accumuloUser = "root";
		accumuloPassword = DEFAULT_MINI_ACCUMULO_PASSWORD;
		accumuloInstance = "miniInstance";
		zookeeper = "localhost:28144";

		accumuloOperations = new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE);

		hdfsBaseDirectory = "file:/C:/cygwin64/tmp/1423263125294-0//test_tmp";
		hdfsProtocol = false;
		hdfs = "file:///";
		jobtracker = DEFAULT_JOB_TRACKER;

	}

	protected static Configuration getConfiguration() {
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfs);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(
				"mapred.job.tracker",
				jobtracker);
		// for travis-ci to run, we want to limit the memory consumption
		conf.setInt(
				MRJobConfig.IO_SORT_MB,
				10);
		return conf;
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}

	final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
			new FeatureCentroidDistanceFn(),
			getBuilder());


	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		 runDBScan(null);
	}

	private void runDBScan(
			final DistributableQuery query )
			throws Exception {

		final GeowaveRDD jobRunner = new GeowaveRDD();
		final int res = jobRunner.run(
				getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							GlobalParameters.Global.ZOOKEEKER,
							GlobalParameters.Global.ACCUMULO_INSTANCE,
							GlobalParameters.Global.ACCUMULO_USER,
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
							GlobalParameters.Global.BATCH_ID,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							2,
							"centroid",
							zookeeper,
							accumuloInstance,
							accumuloUser,
							accumuloPassword,
							TEST_NAMESPACE,
							"bx1",
							hdfsBaseDirectory + "/t1",
							3,
							2
						}));

		Assert.assertEquals(
				0,
				res);
		final int resultCounLevel1 = countResults(
				"bx1",
				1, // level
				1);
		final int resultCounLevel2 = countResults(
				"bx1",
				2, // level
				resultCounLevel1);
		Assert.assertTrue(resultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int countResults(
			final String batchID,
			final int level,
			final int expectedParentCount )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {

		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"centroid",
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchID,
				level);

		final CentroidManager<SimpleFeature> hullManager = new CentroidManagerGeoWave<SimpleFeature>(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"convex_hull",
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchID,
				level);

		int childCount = 0;
		int parentCount = 0;
		for (final String grp : centroidManager.getAllCentroidGroups()) {
			final List<AnalyticItemWrapper<SimpleFeature>> centroids = centroidManager.getCentroidsForGroup(grp);
			final List<AnalyticItemWrapper<SimpleFeature>> hulls = hullManager.getCentroidsForGroup(grp);
			for (final AnalyticItemWrapper<SimpleFeature> centroid : centroids) {
				Assert.assertTrue(centroid.getGeometry() != null);
				Assert.assertTrue(centroid.getBatchID() != null);
				boolean found = false;
				for (final AnalyticItemWrapper<SimpleFeature> hull : hulls) {
					found |= (hull.getName().equals(centroid.getName()));
					Assert.assertTrue(hull.getGeometry() != null);
					Assert.assertTrue(hull.getBatchID() != null);
				}
				Assert.assertTrue(
						grp,
						found);
				childCount++;
			}
			parentCount++;
		}
		Assert.assertEquals(
				batchID,
				expectedParentCount,
				parentCount);
		return childCount;

	}
}
