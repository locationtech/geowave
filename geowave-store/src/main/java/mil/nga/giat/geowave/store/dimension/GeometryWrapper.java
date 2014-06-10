package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.store.index.CommonIndexValue;

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

}
