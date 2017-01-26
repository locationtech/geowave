package mil.nga.giat.geowave.analytic.distance;

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FeatureDistanceFnTest
{

	FeatureDistanceFn functionUnderTest = new FeatureDistanceFn();
	SimpleFeatureType featureType;
	final GeometryFactory factory = new GeometryFactory();

	@Before
	public void setup() {
		featureType = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();

	}

	@Test
	public void testPoint() {
		final SimpleFeature feature1 = createFeature(factory.createPoint(new Coordinate(
				0,
				0)));
		final SimpleFeature feature2 = createFeature(factory.createPoint(new Coordinate(
				0.001,
				0.001)));
		testBounds(
				functionUnderTest.measure(
						feature1,
						feature2),
				100,
				200);
	}

	@Test
	public void testPointWithPoly() {

		final SimpleFeature feature1 = createFeature(factory.createPoint(new Coordinate(
				0,
				0)));
		final SimpleFeature feature2 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.001,
					0.001),
			new Coordinate(
					0.001,
					0.002),
			new Coordinate(
					0.002,
					0.002),
			new Coordinate(
					0.001,
					0.001)
		}));
		testBounds(
				functionUnderTest.measure(
						feature1,
						feature2),
				100,
				200);

	}

	@Test
	public void testPolyWithPoly() {

		final SimpleFeature feature1 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.000,
					0.000),
			new Coordinate(
					-0.000,
					-0.001),
			new Coordinate(
					-0.001,
					-0.001),
			new Coordinate(
					0.00,
					0.00)
		}));
		final SimpleFeature feature2 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.001,
					0.001),
			new Coordinate(
					0.001,
					0.002),
			new Coordinate(
					0.002,
					0.002),
			new Coordinate(
					0.001,
					0.001)
		}));

		testBounds(
				functionUnderTest.measure(
						feature1,
						feature2),
				100,
				200);

	}

	@Test
	public void testIntersectingPoly() {

		final SimpleFeature feature1 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.000,
					0.000),
			new Coordinate(
					0.0012,
					0.000),
			new Coordinate(
					0.0013,
					0.0015),
			new Coordinate(
					0.00,
					0.00)
		}));
		final SimpleFeature feature2 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.001,
					0.001),
			new Coordinate(
					0.002,
					0.001),
			new Coordinate(
					0.002,
					0.002),
			new Coordinate(
					0.001,
					0.001)
		}));

		testBounds(
				functionUnderTest.measure(
						feature1,
						feature2),
				0,
				0.00001);

		final SimpleFeature feature3 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.000,
					0.000),
			new Coordinate(
					0.001,
					0.001),
			new Coordinate(
					0.000,
					0.001),
			new Coordinate(
					0.00,
					0.00)
		}));
		final SimpleFeature feature4 = createFeature(factory.createPolygon(new Coordinate[] {
			new Coordinate(
					0.001,
					0.001),
			new Coordinate(
					0.002,
					0.001),
			new Coordinate(
					0.002,
					0.002),
			new Coordinate(
					0.001,
					0.001)
		}));
		testBounds(
				functionUnderTest.measure(
						feature3,
						feature4),
				0.0,
				0.00001);

	}

	private void testBounds(
			final double distance,
			final double lower,
			final double upper ) {
		assertTrue((distance >= lower) && (distance <= upper));
	}

	private SimpleFeature createFeature(
			final Geometry geometry ) {
		return AnalyticFeature.createGeometryFeature(
				featureType,
				"b1",
				UUID.randomUUID().toString(),
				UUID.randomUUID().toString(),
				"NA",
				20.30203,
				geometry,
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
	}
}
