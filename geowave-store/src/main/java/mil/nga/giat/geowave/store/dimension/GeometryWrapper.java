package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

/**
 * This class wraps JTS geometry with visibility so that it can be used within
 * GeoWave as a CommonIndexValue
 * 
 */
public class GeometryWrapper implements
		CommonIndexValue
{
	private byte[] visibility;
	private final com.vividsolutions.jts.geom.Geometry geometry;

	public GeometryWrapper(
			final com.vividsolutions.jts.geom.Geometry geometry ) {
		this.geometry = geometry;
	}

	public GeometryWrapper(
			final com.vividsolutions.jts.geom.Geometry geometry,
			final byte[] visibility ) {
		this.visibility = visibility;
		this.geometry = geometry;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	public com.vividsolutions.jts.geom.Geometry getGeometry() {
		return geometry;
	}

	/**
	 * Expects Longitude before Latitude
	 */
	@Override
	public boolean overlaps(
			final DimensionField[] fields,
			final NumericData[] rangeData ) {

		int latPosition = fields[0] instanceof LatitudeField ? 0 : 1;
		int longPosition = fields[0] instanceof LatitudeField ? 1 : 0;
		if (fields.length == 1) {
			Envelope env = geometry.getEnvelopeInternal();
			NumericRange r = latPosition == 0 ? new NumericRange(
					env.getMinY(),
					env.getMaxY()) : new NumericRange(
					env.getMinX(),
					env.getMaxX());
			double t0 = rangeData[0].getMax() - r.getMin();
			double t1 = r.getMax() - rangeData[0].getMin();
			return Math.abs(t0 - t1) <= (t0 + t1);
		}
		return geometry.getFactory().createPolygon(
				new Coordinate[] {
					new Coordinate(
							rangeData[longPosition].getMin(),
							rangeData[latPosition].getMin()),
					new Coordinate(
							rangeData[longPosition].getMin(),
							rangeData[latPosition].getMax()),
					new Coordinate(
							rangeData[longPosition].getMax(),
							rangeData[latPosition].getMax()),
					new Coordinate(
							rangeData[longPosition].getMax(),
							rangeData[latPosition].getMin()),
					new Coordinate(
							rangeData[longPosition].getMin(),
							rangeData[latPosition].getMin())
				}).intersects(
				geometry);
	}
}
