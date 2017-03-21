package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
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

	private IndexedPersistenceEncoding createData(
			final Geometry geomData ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		commonData.addValue(new PersistentValue<CommonIndexValue>(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID,
				new GeometryWrapper(
						geomData)));

		return new IndexedPersistenceEncoding(
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				1,
				commonData,
				new PersistentDataset<byte[]>());
	}

	public void performOp(
			final CompareOperation op,
			final boolean[] expectedResults ) {
		final GeometryFactory factory = new GeometryFactory();
		// query geometry for testing
		Coordinate[] queryCoord = new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					37),
			new Coordinate(
					24,
					37),
			new Coordinate(
					24,
					33)
		};
		// create spatial query object with geometric relationship operator
		final SpatialQuery query = new SpatialQuery(
				factory.createPolygon(queryCoord),
				op);

		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());

		// This line is crossing query polygon
		final Coordinate[] line1 = new Coordinate[] {
			new Coordinate(
					22,
					32),
			new Coordinate(
					25,
					36)
		};
		// This line is completely within the query polygon
		final Coordinate[] line2 = new Coordinate[] {
			new Coordinate(
					25,
					33.5),
			new Coordinate(
					26,
					34)
		};
		// This line is completely outside of the query polygon
		final Coordinate[] line3 = new Coordinate[] {
			new Coordinate(
					21,
					33.5),
			new Coordinate(
					23,
					34)
		};
		// This line is touching one of the corner of the query polygon
		final Coordinate[] line4 = new Coordinate[] {
			new Coordinate(
					28,
					33),
			new Coordinate(
					30,
					34)
		};
		// this polygon is completely contained within the query polygon
		final Coordinate[] smallPolygon = new Coordinate[] {
			new Coordinate(
					25,
					34),
			new Coordinate(
					27,
					34),
			new Coordinate(
					27,
					36),
			new Coordinate(
					25,
					36),
			new Coordinate(
					25,
					34)
		};

		// this polygon is same as query polygon
		final Coordinate[] dataPolygon = queryCoord.clone();

		final IndexedPersistenceEncoding[] data = new IndexedPersistenceEncoding[] {
			createData(factory.createLineString(line1)),
			createData(factory.createLineString(line2)),
			createData(factory.createLineString(line3)),
			createData(factory.createLineString(line4)),
			createData(factory.createPolygon(smallPolygon)),
			createData(factory.createPolygon(dataPolygon))
		};

		int pos = 0;
		final CommonIndexModel model = new SpatialDimensionalityTypeProvider().createPrimaryIndex().getIndexModel();
		for (final IndexedPersistenceEncoding dataItem : data) {
			for (final QueryFilter filter : queryCopy.createFilters(model)) {
				assertEquals(
						"result: " + pos,
						expectedResults[pos++],
						filter.accept(
								model,
								dataItem));
			}
		}
	}

	@Test
	public void testContains() {
		performOp(
				CompareOperation.CONTAINS,
				new boolean[] {
					false,
					true,
					false,
					false,
					true,
					true
				});
	}

	@Test
	public void testOverlaps() {
		performOp(
				CompareOperation.OVERLAPS,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testIntersects() {
		performOp(
				CompareOperation.INTERSECTS,
				new boolean[] {
					true,
					true,
					false,
					true,
					true,
					true
				});
	}

	@Test
	public void testDisjoint() {
		performOp(
				CompareOperation.DISJOINT,
				new boolean[] {
					false,
					false,
					true,
					false,
					false,
					false
				});
	}

	@Test
	public void testTouches() {
		performOp(
				CompareOperation.TOUCHES,
				new boolean[] {
					false,
					false,
					false,
					true,
					false,
					false
				});
	}

	@Test
	public void testCrosses() {
		performOp(
				CompareOperation.CROSSES,
				new boolean[] {
					true,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testWithin() {
		performOp(
				CompareOperation.WITHIN,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					true
				});
	}

	@Test
	public void testEquals() {
		performOp(
				CompareOperation.EQUALS,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					true
				});
	}
}
