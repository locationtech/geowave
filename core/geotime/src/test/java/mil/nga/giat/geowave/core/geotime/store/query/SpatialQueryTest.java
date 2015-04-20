package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SpatialQueryTest
{
	@Test
	public void test() {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialQuery query = new SpatialQuery(
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							24,
							33),
					new Coordinate(
							28,
							33),
					new Coordinate(
							28,
							31),
					new Coordinate(
							24,
							31),
					new Coordinate(
							24,
							33)
				}));
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());
		assertEquals(
				queryCopy.getQueryGeometry(),
				query.getQueryGeometry());
	}
}
