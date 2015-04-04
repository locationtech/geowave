package mil.nga.giat.geowave.analytics.distance;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateCosineDistanceFn implements
		DistanceFn<Coordinate>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2074200104626591273L;

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		final double ab = (x.x * y.x) + (x.y * y.y) + (x.z * y.z);
		final double norma = Math.sqrt(Math.pow(
				x.x,
				2) + Math.pow(
				x.y,
				2) + Math.pow(
				x.z,
				2));
		final double normb = Math.sqrt(Math.pow(
				y.x,
				2) + Math.pow(
				y.y,
				2) + Math.pow(
				y.z,
				2));
		return ab / (norma * normb);
	}

}
