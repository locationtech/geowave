package mil.nga.giat.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTS;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;

public class ExtractGeometryFilterVisitorTest
{
	final ExtractGeometryFilterVisitor visitorWithDescriptor = (ExtractGeometryFilterVisitor) ExtractGeometryFilterVisitor.GEOMETRY_VISITOR;

	@Test
	public void testDWithin()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("DWITHIN(geom, POINT(-122.7668 0.4979), 233.7, meters)");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		final Geometry geometry = result.getGeometry();
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

	@Test
	public void testDWithinDateLine()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("DWITHIN(geom, POINT(179.9998 0.79), 13.7, kilometers)");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		final Geometry geometry = result.getGeometry();
		assertNotNull(geometry);
		for (Coordinate coord : geometry.getCoordinates()) {

			assertEquals(
					13707.1,
					JTS.orthodromicDistance(
							coord,
							new Coordinate(
									179.9999,
									0.79),
							GeoWaveGTDataStore.DEFAULT_CRS),
					2000);
		}
	}

	@Test
	public void testBBOX()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("BBOX(geom, 0, 0, 10, 25)");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testIntersects()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("INTERSECTS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testOverlaps()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("OVERLAPS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.OVERLAPS);
	}

	@Test
	public void testEquals()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("EQUALS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.EQUALS);
	}

	@Test
	public void testCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("CROSSES(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.CROSSES);
	}

	@Test
	public void testTouches()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("TOUCHES(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.TOUCHES);
	}

	@Test
	public void testWithin()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("WITHIN(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.CONTAINS);
	}

	@Test
	public void testContains()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("CONTAINS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.WITHIN);
	}

	@Test
	public void testDisjoint()
			throws CQLException,
			TransformException,
			ParseException {

		Filter filter = CQL.toFilter("DISJOINT(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		// for non-inclusive filters we can't extract query geometry and
		// predicate
		// assertTrue(Double.isNaN(result.getGeometry().getArea()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testIntesectAndBBox()
			throws CQLException,
			TransformException,
			ParseException {

		// BBOX geometry is completely contained within Intersects geometry
		// we are testing to see if we are able to combine simple geometric
		// relations with similar predicates
		// into a single query geometry/predicate
		Filter filter = CQL
				.toFilter("INTERSECTS(geom, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) AND BBOX(geom, 0, 0, 10, 25)");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testIntesectAndCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		// CROSSES geometry is completely contained within INTERSECT geometry
		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, we can't combine two different
		// predicates into one but
		// we can combine geometries for the purpose of deriving linear
		// constraints
		Filter filter = CQL
				.toFilter("INTERSECTS(geom, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) AND CROSSES(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testOverlapsOrCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		// TOUCHES geometry is completely contained within OVERLAPS geometry
		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, we can't combine two different
		// predicates into one but
		// we can combine geometries for the purpose of deriving linear
		// constraints
		Filter filter = CQL
				.toFilter("OVERLAPS(geom, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) OR TOUCHES(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				20,
				0,
				50);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testIntesectAndCrossesAndLike()
			throws CQLException,
			TransformException,
			ParseException {

		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, that returned predicate is null
		// since we can't represent
		// CQL expression fully into single query geometry and predicate
		Filter filter = CQL.toFilter("CROSSES(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0))) AND location == 'abc'");
		Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

}
