package mil.nga.giat.geowave.core.geotime.store.dimension;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class GeometryWrapperTest
{

	private final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FLOATING));

	@Test
	public void testLatitutde() {
		DimensionField[] fields = new DimensionField[] {
			new LatitudeField()
		};

		Geometry geo = factory.createLineString(new Coordinate[] {
			new Coordinate(
					-99.22,
					33.75000000000001), // notice that
										// this gets
										// tiled as
										// 33.75
			new Coordinate(
					-99.15,
					33.75000000000001)
		// notice that this gets tiled as 33.75
				});

		GeometryWrapper wrapper = new GeometryWrapper(
				geo);

		NumericRange rangeData = new NumericRange(
				33.7442334433,
				33.75 + (1E-10d));
		assertTrue(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));
		rangeData = new NumericRange(
				33.7442334433,
				33.75 - (1E-10d));
		assertFalse(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));

		rangeData = new NumericRange(
				33.75 - (1E-10d),
				33.751);
		assertTrue(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));
		rangeData = new NumericRange(
				33.75 + (1E-10d),
				33.751);
		assertFalse(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));
	}

	@Test
	public void testLongitude() {
		DimensionField[] fields = new DimensionField[] {
			new LongitudeField()
		};

		Geometry geo = factory.createLineString(new Coordinate[] {
			new Coordinate(
					-99.22,
					33.75000000000001), // notice that
										// this gets
										// tiled as
										// 33.75
			new Coordinate(
					-99.15,
					33.75000000000001)
		// notice that this gets tiled as 33.75
				});

		GeometryWrapper wrapper = new GeometryWrapper(
				geo);

		NumericRange rangeData = new NumericRange(
				-99.15 - (1E-10d),
				-99.140348473);
		assertTrue(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));
		rangeData = new NumericRange(
				-99.15 + (1E-10d),
				-99.140348473);
		assertFalse(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));

		rangeData = new NumericRange(
				-99.23,
				-99.22 + (1E-9d));
		assertTrue(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));
		rangeData = new NumericRange(
				-99.23,
				-99.22 - (1E-10d));
		assertFalse(wrapper.overlaps(
				fields,
				new NumericData[] {
					rangeData
				}));

	}

}
