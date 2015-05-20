package mil.nga.giat.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.text.ParseException;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTS;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class ExtractGeometryFilterVisitorTest
{
	final ExtractGeometryFilterVisitor visitorWithDescriptor = (ExtractGeometryFilterVisitor) ExtractGeometryFilterVisitor.GEOMETRY_VISITOR;

	@Test
	public void testAfter()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("DWITHIN(geom, POINT(-122.7668 0.4979), 233.7, meters)");
		Query query = new Query(
				"type",
				filter);

		Geometry geometry = (Geometry) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(geometry);
		for (Coordinate coord : geometry.getCoordinates()) {

			assertEquals(
					233.7,
					JTS.orthodromicDistance(
							coord,
							new Coordinate(
									-122.7668,
									0.4979),
							GeoWaveGTDataStore.DEFAULT_CRS),
					2);
		}
	}

}
