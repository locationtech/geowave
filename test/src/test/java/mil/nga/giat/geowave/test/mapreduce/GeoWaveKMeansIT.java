package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class GeoWaveKMeansIT extends
		MapReduceTestEnvironment
{

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

	private void testIngest()
			throws IOException {

		dataGenerator.writeToGeoWave(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
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
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
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
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
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

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		testIngest();

		runKPlusPlus(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
		// runKJumpPlusPlus(new SpatialQuery(
		// dataGenerator.getBoundingRegion()));
		// GeoWaveTestEnvironment.accumuloOperations.deleteAll();
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
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							query,
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							"2",
							2,
							false,
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
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
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
							zookeeper,
							accumuloInstance,
							accumuloUser,
							accumuloPassword,
							TEST_NAMESPACE,
							"bx2",
							hdfsBaseDirectory + "/t2",
							new NumericRange(
									4,
									7),
							5,
							2
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
			final String batchID,
			final int level,
			final int expectedParentCount )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {

		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				GeoWaveTestEnvironment.zookeeper,
				GeoWaveTestEnvironment.accumuloInstance,
				GeoWaveTestEnvironment.accumuloUser,
				GeoWaveTestEnvironment.accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"centroid",
				IndexType.SPATIAL_VECTOR.getDefaultId(),
				batchID,
				level);

		final CentroidManager<SimpleFeature> hullManager = new CentroidManagerGeoWave<SimpleFeature>(
				GeoWaveTestEnvironment.zookeeper,
				GeoWaveTestEnvironment.accumuloInstance,
				GeoWaveTestEnvironment.accumuloUser,
				GeoWaveTestEnvironment.accumuloPassword,
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
				if (centroid.getAssociationCount() == 0) {
					continue;
				}
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
