package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
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

public class GeoWavePDBSCANIT extends
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
		/*
		 * dataGenerator.writeToGeoWave( zookeeper, accumuloInstance,
		 * accumuloUser, accumuloPassword, TEST_NAMESPACE,
		 * dataGenerator.generatePointSet( 0.15, 0.2, 6, 600, new double[] { 0,
		 * 0 }, new double[] { 10, 10 })); dataGenerator.writeToGeoWave(
		 * zookeeper, accumuloInstance, accumuloUser, accumuloPassword,
		 * TEST_NAMESPACE, dataGenerator.generatePointSet( 0.15, 0.2, 4, 900,
		 * new double[] { 65, 35 }, new double[] { 75, 45 }));
		 */
	}

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		testIngest();
		System.out.println(hdfs);
		System.out.println(hdfsBaseDirectory);
		System.out.println(GeoWaveTestEnvironment.zookeeper);
		System.out.println(GeoWaveTestEnvironment.accumuloInstance);
		Thread.sleep(Long.MAX_VALUE);
		// runDBScan(new SpatialQuery(
		// dataGenerator.getBoundingRegion()));
	}

	private void runDBScan(
			final DistributableQuery query )
			throws Exception {

		final GeowaveRDD jobRunner = new GeowaveRDD();
		final int res = jobRunner.run(
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
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							query,
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
