package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

public class GeometryUtilsTest
{

	GeometryFactory factory = new GeometryFactory();

	@Test
	public void testSplit() {
		Geometry multiPolygon = factory.createMultiPolygon(new Polygon[] {
			factory.createPolygon(new Coordinate[] {
				new Coordinate(
						179.0,
						-89),
				new Coordinate(
						179.0,
						-92),
				new Coordinate(
						182.0,
						-92),
				new Coordinate(
						192.0,
						-89),
				new Coordinate(
						179.0,
						-89)
			})
		});
		Geometry result = GeometryUtils.adjustGeo(
				GeoWaveGTDataStore.DEFAULT_CRS,
				multiPolygon);

		assertTrue(result.intersects(multiPolygon));
		assertTrue(result.getNumGeometries() == 2);
	}

	@Test
	public void testSimple() {

		Geometry singlePoly = factory.createMultiPolygon(new Polygon[] {
			factory.createPolygon(new Coordinate[] {
				new Coordinate(
						169.0,
						20),
				new Coordinate(
						169.0,
						21),
				new Coordinate(
						172.0,
						21),
				new Coordinate(
						172.0,
						20),
				new Coordinate(
						169.0,
						20)
			})
		});
		Geometry result = GeometryUtils.adjustGeo(
				GeoWaveGTDataStore.DEFAULT_CRS,
				singlePoly);

		assertTrue(result.intersects(singlePoly));
		assertTrue(singlePoly.isValid());
		assertTrue(singlePoly.getNumGeometries() == 1);

	}
}
