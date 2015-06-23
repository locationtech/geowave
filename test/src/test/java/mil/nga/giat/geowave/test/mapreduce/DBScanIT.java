package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ShapefileTool;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.DBScanIterationsJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class DBScanIT extends
		MapReduceTestEnvironment
{

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setSRS(ClusteringUtils.CLUSTERING_CRS);
		try {
			typeBuilder.setCRS(CRS.decode(
					ClusteringUtils.CLUSTERING_CRS,
					true));
		}
		catch (FactoryException e) {
			e.printStackTrace();
			return null;
		}
		// add attributes in order
		typeBuilder.add(
				"geom",
				Point.class);
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
			new FeatureCentroidOrthodromicDistanceFn(),
			getBuilder());

	@Test
	public void testDBScan()
			throws Exception {
		dataGenerator.setIncludePolygons(false);
		ingest();
		runScan(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
	}

	private void runScan(
			final DistributableQuery query )
			throws Exception {

		final DBScanIterationsJobRunner jobRunner = new DBScanIterationsJobRunner();
		final int res = jobRunner.run(
				getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.QUERY_OPTIONS,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							PartitionParameters.Partition.PARTITION_DISTANCE,
							PartitionParameters.Partition.PARTITIONER_CLASS,
							ClusteringParameters.Clustering.MINIMUM_SIZE,
							GlobalParameters.Global.ZOOKEEKER,
							GlobalParameters.Global.ACCUMULO_INSTANCE,
							GlobalParameters.Global.ACCUMULO_USER,
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							OutputParameters.Output.REDUCER_COUNT,
							InputParameters.Input.INPUT_FORMAT,
							GlobalParameters.Global.BATCH_ID
						},
						new Object[] {
							query,
							new QueryOptions(
									"geom"),
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							10000,
							OrthodromicDistancePartitioner.class,
							10,
							zookeeper,
							accumuloInstance,
							accumuloUser,
							accumuloPassword,
							TEST_NAMESPACE,
							hdfsBaseDirectory + "/t1",
							2,
							GeoWaveInputFormatConfiguration.class,
							"bx5"
						}));

		Assert.assertEquals(
				0,
				res);

		Assert.assertTrue(readHulls() > 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int readHulls()
			throws Exception {
		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
				new SimpleFeatureItemWrapperFactory(),
				"concave_hull",
				"hull_idx",
				"bx5",
				0);

		int count = 0;
		for (String grp : centroidManager.getAllCentroidGroups()) {
			for (AnalyticItemWrapper<SimpleFeature> feature : centroidManager.getCentroidsForGroup(grp)) {
				ShapefileTool.writeShape(
						feature.getName(),
						new File(
								"./target/test_final_" + feature.getName()),
						new Geometry[] {
							feature.getGeometry()
						});
				count++;

			}
		}
		return count;
	}

	private void ingest()
			throws IOException {
		final List<SimpleFeature> features = dataGenerator.generatePointSet(
				0.05,
				0.5,
				4,
				800,
				new double[] {
					-86,
					-30
				},
				new double[] {
					-90,
					-34
				});

		features.addAll(dataGenerator.generatePointSet(
				dataGenerator.getFactory().createLineString(
						new Coordinate[] {
							new Coordinate(
									-87,
									-32),
							new Coordinate(
									-87.5,
									-32.3),
							new Coordinate(
									-87.2,
									-32.7)
						}),
				0.2,
				500));

		ShapefileTool.writeShape(
				new File(
						"./target/test_in"),
				features);
		dataGenerator.writeToGeoWave(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
				features);
	}
}