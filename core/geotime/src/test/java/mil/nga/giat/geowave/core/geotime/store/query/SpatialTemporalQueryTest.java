package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.TimeRange;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SpatialTemporalQueryTest
{
	SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssz");

	@Test
	public void test()
			throws ParseException {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialTemporalQuery query = new SpatialTemporalQuery(
				df.parse("2005-05-17T19:32:56GMT-00:00"),
				df.parse("2005-05-17T22:32:56GMT-00:00"),
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
		final SpatialTemporalQuery queryCopy = new SpatialTemporalQuery();
		queryCopy.fromBinary(query.toBinary());
		assertEquals(
				queryCopy.getQueryGeometry(),
				query.getQueryGeometry());
	}

	private IndexedPersistenceEncoding createData(
			Date start,
			Date end,
			Coordinate[] coordinates ) {
		GeometryFactory factory = new GeometryFactory();
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		commonData.addOrUpdateValue(new PersistentValue<CommonIndexValue>(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID,
				new GeometryWrapper(
						factory.createLineString(coordinates))));
		commonData.addOrUpdateValue(new PersistentValue<CommonIndexValue>(
				new TimeField(
						Unit.YEAR).getFieldId(),
				new TimeRange(
						start.getTime(),
						end.getTime(),
						new byte[0])));

		return new IndexedPersistenceEncoding(
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				1,
				commonData);
	}

	public void performOp(
			CompareOperation op,
			boolean[] expectedResults )
			throws ParseException {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialTemporalQuery query = new SpatialTemporalQuery(
				df.parse("2005-05-17T19:32:56GMT-00:00"),
				df.parse("2005-05-17T22:32:56GMT-00:00"),
				factory.createPolygon(new Coordinate[] {
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
				}),
				op);
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());

		IndexedPersistenceEncoding[] data = new IndexedPersistenceEncoding[] {
			createData(
					df.parse("2005-05-17T19:32:56GMT-00:00"),
					df.parse("2005-05-17T22:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T17:32:56GMT-00:00"),
					df.parse("2005-05-17T21:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T19:33:56GMT-00:00"),
					df.parse("2005-05-17T20:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T16:32:56GMT-00:00"),
					df.parse("2005-05-17T21:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T22:33:56GMT-00:00"),
					df.parse("2005-05-17T22:34:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					})
		};

		int pos = 0;
		for (IndexedPersistenceEncoding dataItem : data)
			for (QueryFilter filter : queryCopy.createFilters(IndexType.SPATIAL_TEMPORAL_VECTOR.getDefaultIndexModel())) {
				assertEquals(
						"result: " + (pos + 1),
						expectedResults[pos++],
						filter.accept(dataItem));
			}
	}

	@Test
	public void testContains()
			throws ParseException {
		performOp(
				CompareOperation.CONTAINS,
				new boolean[] {
					true,
					false,
					true,
					false,
					false
				});
	}

	@Test
	public void testOverlaps()
			throws ParseException {
		performOp(
				CompareOperation.OVERLAPS,
				new boolean[] {
					true,
					true,
					true,
					true,
					false
				});
	}
}
