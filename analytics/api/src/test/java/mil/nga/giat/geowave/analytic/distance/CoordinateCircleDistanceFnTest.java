package mil.nga.giat.geowave.analytic.distance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateCircleDistanceFnTest
{

	@Test
	public void test() {
		final CoordinateCircleDistanceFn fn = new CoordinateCircleDistanceFn();
		double d1 = fn.measure(
				new Coordinate(
						90,
						0),
				new Coordinate(
						89,
						0));
		double d2 = fn.measure(
				new Coordinate(
						89,
						0),
				new Coordinate(
						90,
						0));
		double d3close = fn.measure(
				new Coordinate(
						10.000000001,
						89.00000010),
				new Coordinate(
						10.000000002,
						89.00000001));
		double dateLineclose = fn.measure(
				new Coordinate(
						-179.9999999,
						0.00001),
				new Coordinate(
						179.9999999,
						0.00001));
		assertEquals(
				d1,
				d2,
				0.0000001);
		assertEquals(
				111319.49079322655,
				d1,
				0.00001);
		assertTrue(d3close < 0.04);
		assertTrue(dateLineclose < 0.03);

	}
}
