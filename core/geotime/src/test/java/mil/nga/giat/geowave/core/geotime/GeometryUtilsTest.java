package mil.nga.giat.geowave.core.geotime;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;

import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class GeometryUtilsTest
{

	private Point point3D;
	private Point point2D;

	@Before
	public void createGeometry() {

		final GeometryFactory gf = new GeometryFactory();

		point2D = gf.createPoint(new Coordinate(
				1,
				2));

		point3D = gf.createPoint(new Coordinate(
				1,
				2,
				3));
	}

	@Test
	public void test2DGeometryBinaryConversion() {

		// convert 2D point to binary representation
		final byte[] bytes = GeometryUtils.geometryToBinary(point2D);

		// load the converted 2D geometry
		final Geometry convGeo = GeometryUtils.geometryFromBinary(bytes);

		// get the coordinates for each version
		final Coordinate origCoords = point2D.getCoordinates()[0];
		final Coordinate convCoords = convGeo.getCoordinates()[0];

		Assert.assertEquals(
				origCoords.x,
				convCoords.x);

		Assert.assertEquals(
				origCoords.y,
				convCoords.y);

		Assert.assertTrue(Double.isNaN(convCoords.getOrdinate(Coordinate.Z)));
	}

	@Test
	public void test3DGeometryBinaryConversion() {

		// convert 3D point to binary representation
		final byte[] bytes = GeometryUtils.geometryToBinary(point3D);

		// load the converted 3D geometry
		final Geometry convGeo = GeometryUtils.geometryFromBinary(bytes);

		// get the coordinates for each version
		final Coordinate origCoords = point3D.getCoordinates()[0];
		final Coordinate convCoords = convGeo.getCoordinates()[0];

		Assert.assertEquals(
				origCoords.x,
				convCoords.x);

		Assert.assertEquals(
				origCoords.y,
				convCoords.y);

		Assert.assertEquals(
				origCoords.z,
				convCoords.z);
	}
}
