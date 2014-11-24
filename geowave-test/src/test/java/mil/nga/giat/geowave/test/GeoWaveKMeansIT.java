package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.runners.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytics.clustering.runners.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytics.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.JumpParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class GeoWaveKMeansIT extends
		GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveKMeansIT.class);
	private static final String HDFS_BASE_DIRECTORY = "test_tmp";
	private static final String DEFAULT_JOB_TRACKER = "local";

	private static final int MIN_INPUT_SPLITS = 2;
	private static final int MAX_INPUT_SPLITS = 4;
	protected static String jobtracker;
	protected static String hdfs;
	protected static boolean hdfsProtocol;
	private static String hdfsBaseDirectory;

	public static enum ResultCounterType {
		EXPECTED,
		UNEXPECTED,
		ERROR
	};

	@BeforeClass
	public static void setVariables()
			throws MalformedURLException {
		hdfs = System.getProperty("hdfs");
		jobtracker = System.getProperty("jobtracker");
		if (!GeoWaveITSuite.isSet(hdfs)) {
			hdfs = "file:///";

			hdfsBaseDirectory = GeoWaveITSuite.tempDir.toURI().toURL().toString() + "/" + HDFS_BASE_DIRECTORY;
			new File(
					hdfsBaseDirectory).deleteOnExit();
			hdfsProtocol = false;
		}
		else {
			hdfsBaseDirectory = HDFS_BASE_DIRECTORY;
			if (!hdfs.contains("://")) {
				hdfs = "hdfs://" + hdfs;
				hdfsProtocol = true;
			}
			else {
				hdfsProtocol = hdfs.toLowerCase().startsWith(
						"hdfs://");
			}
		}
		if (!GeoWaveITSuite.isSet(jobtracker)) {
			jobtracker = DEFAULT_JOB_TRACKER;
		}
	}

	@AfterClass
	public static void cleanupHdfsFiles() {
		if (hdfsProtocol) {
			final Path tmpDir = new Path(
					hdfsBaseDirectory);
			try {
				final FileSystem fs = FileSystem.get(getConfiguration());
				fs.delete(
						tmpDir,
						true);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete HDFS temp directory",
						e);
			}
		}
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

	private void testIngest(
			final String zookeeper,
			final String instance,
			final String user,
			final String password,
			final String namespace )
			throws IOException {

		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						3,
						800,
						new double[] {
							-100,
							-45
						},
						new double[] {
							-90,
							-35
						}));
		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						6,
						600,
						new double[] {
							0,
							0
						},
						new double[] {
							10,
							10
						}));
		dataGenerator.writeToGeoWave(
				zookeeper,
				instance,
				user,
				password,
				namespace,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						4,
						900,
						new double[] {
							65,
							35
						},
						new double[] {
							75,
							45
						}));

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

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		testIngest(
				GeoWaveITSuite.zookeeper,
				GeoWaveITSuite.accumuloInstance,
				GeoWaveITSuite.accumuloUser,
				GeoWaveITSuite.accumuloPassword,
				TEST_NAMESPACE);

		//runKPlusPlus(new SpatialQuery(
		//		dataGenerator.getBoundingRegion()));
		runKJumpPlusPlus(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
		GeoWaveITSuite.accumuloOperations.deleteAll();
	}

	private void runKPlusPlus(
			final DistributableQuery query )
			throws Exception {

		final MultiLevelKMeansClusteringJobRunner jobRunner = new MultiLevelKMeansClusteringJobRunner();
		final int res = jobRunner.run(
				getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ClusteringParameters.Clustering.MAX_ITERATIONS,
							ClusteringParameters.Clustering.RETAIN_GROUP_ASSIGNMENTS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							GlobalParameters.Global.ZOOKEEKER,
							GlobalParameters.Global.ACCUMULO_INSTANCE,
							GlobalParameters.Global.ACCUMULO_USER,
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
							GlobalParameters.Global.BATCH_ID,
							GlobalParameters.Global.HDFS_BASEDIR,
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							query,
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							"2",
							2,
							true,
							"centroid",
							GeoWaveITSuite.zookeeper,
							GeoWaveITSuite.accumuloInstance,
							GeoWaveITSuite.accumuloUser,
							GeoWaveITSuite.accumuloPassword,
							TEST_NAMESPACE,
							"bx1",
							this.hdfsBaseDirectory + "/t1",
							10,
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

	private void runKJumpPlusPlus(
			final DistributableQuery query )
			throws Exception {

		final MultiLevelJumpKMeansClusteringJobRunner jobRunner2 = new MultiLevelJumpKMeansClusteringJobRunner();
		final int res2 = jobRunner2.run(
				getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
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
							GlobalParameters.Global.HDFS_BASEDIR,
							JumpParameters.Jump.RANGE_OF_CENTROIDS,
							JumpParameters.Jump.KPLUSPLUS_MIN,
							ClusteringParameters.Clustering.MAX_ITERATIONS
						},
						new Object[] {
							query,
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							"2",
							"centroid",
							GeoWaveITSuite.zookeeper,
							GeoWaveITSuite.accumuloInstance,
							GeoWaveITSuite.accumuloUser,
							GeoWaveITSuite.accumuloPassword,
							TEST_NAMESPACE,
							"bx2",
							this.hdfsBaseDirectory + "/t2",
							new NumericRange(
									4,
									8),
							5,
							8
						}));

		Assert.assertEquals(
				0,
				res2);
		final int jumpRresultCounLevel1 = countResults(
				"bx2",
				1,
				1);
		final int jumpRresultCounLevel2 = countResults(
				"bx2",
				2,
				jumpRresultCounLevel1);
		Assert.assertTrue(jumpRresultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int countResults(
			String batchID,
			int level,
			int expectedParentCount )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {

		CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				GeoWaveITSuite.zookeeper,
				GeoWaveITSuite.accumuloInstance,
				GeoWaveITSuite.accumuloUser,
				GeoWaveITSuite.accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"centroid",
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchID,
				level);

		CentroidManager<SimpleFeature> hullManager = new CentroidManagerGeoWave<SimpleFeature>(
				GeoWaveITSuite.zookeeper,
				GeoWaveITSuite.accumuloInstance,
				GeoWaveITSuite.accumuloUser,
				GeoWaveITSuite.accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"convex_hull",
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchID,
				level);

		int childCount = 0;
		int parentCount = 0;
		for (String grp : centroidManager.getAllCentroidGroups()) {
			List<AnalyticItemWrapper<SimpleFeature>> centroids = centroidManager.getCentroidsForGroup(grp);
			List<AnalyticItemWrapper<SimpleFeature>> hulls = hullManager.getCentroidsForGroup(grp);
			for (AnalyticItemWrapper<SimpleFeature> centroid : centroids) {
				Assert.assertTrue(centroid.getGeometry() != null);
				Assert.assertTrue(centroid.getBatchID() != null);
				boolean found = false;
				for (AnalyticItemWrapper<SimpleFeature> hull : hulls) {
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
