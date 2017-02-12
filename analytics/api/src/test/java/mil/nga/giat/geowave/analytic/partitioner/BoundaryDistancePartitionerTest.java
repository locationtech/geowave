package mil.nga.giat.geowave.analytic.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.model.SpatialIndexModelBuilder;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class BoundaryDistancePartitionerTest
{
	public static CoordinateReferenceSystem DEFAULT_CRS;

	static {
		try {
			DEFAULT_CRS = CRS.decode(
					"EPSG:4326",
					true);
		}
		catch (final FactoryException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test()
			throws IOException,
			ClassNotFoundException {

		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();
		final GeometryFactory factory = new GeometryFactory();
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						0,
						0)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		final PropertyManagement propertyManagement = new PropertyManagement();

		propertyManagement.store(
				PartitionParameters.Partition.DISTANCE_THRESHOLDS,
				"10000");

		propertyManagement.store(
				CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS,
				SpatialIndexModelBuilder.class);

		propertyManagement.store(
				ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);
		propertyManagement.store(
				GlobalParameters.Global.CRS_ID,
				"EPSG:4326");
		propertyManagement.store(
				PartitionParameters.Partition.GEOMETRIC_DISTANCE_UNIT,
				"m");

		final BoundaryPartitioner partitioner = new BoundaryPartitioner();
		final Configuration configuration = new Configuration();
		final Class<?> scope = BoundaryDistancePartitionerTest.class;
		propertyManagement.setJobConfiguration(
				configuration,
				scope);
		partitioner.initialize(
				Job.getInstance(configuration),
				scope);

		List<PartitionData> partitions = partitioner.getCubeIdentifiers(feature);
		assertEquals(
				4,
				partitions.size());
		assertTrue(hasNPrimary(
				partitions,
				1));

		for (final PartitionData partition : partitions) {
			final MultiDimensionalNumericData ranges = partitioner.getRangesForPartition(partition);
			assertTrue(ranges.getDataPerDimension()[0].getMin() < 0.0000000001);
			assertTrue(ranges.getDataPerDimension()[0].getMax() > -0.0000000001);
			assertTrue(ranges.getDataPerDimension()[1].getMin() < 0.00000000001);
			assertTrue(ranges.getDataPerDimension()[1].getMax() > -0.0000000001);
		}

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						-179.99999996,
						0)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		partitions = partitioner.getCubeIdentifiers(feature);
		assertEquals(
				4,
				partitions.size());
		assertTrue(hasNPrimary(
				partitions,
				1));

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createLinearRing(new Coordinate[] {
					new Coordinate(
							88,
							0),
					new Coordinate(
							88,
							0.001),
					new Coordinate(
							88.001,
							0.001),
					new Coordinate(
							88.001,
							0),
					new Coordinate(
							88,
							0)
				}),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		partitions = partitioner.getCubeIdentifiers(feature);
		assertTrue(hasNPrimary(
				partitions,
				4));

	}

	private boolean hasNPrimary(
			final List<PartitionData> data,
			int expected ) {
		int count = 0;
		for (final PartitionData dataitem : data) {
			count += (dataitem.isPrimary() ? 1 : 0);
		}
		return count == expected;
	}
}
