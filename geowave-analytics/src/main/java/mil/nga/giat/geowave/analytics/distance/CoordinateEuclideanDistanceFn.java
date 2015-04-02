package mil.nga.giat.geowave.analytics.distance;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateEuclideanDistanceFn implements
		DistanceFn<Coordinate>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 888639577783179566L;

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		return Math.sqrt(Math.pow(
				(x.x - y.x),
				2) + Math.pow(
				(x.y - y.y),
				2) + Math.pow(
				(filter(x.z) - filter(y.z)),
				2));
	}

	private static double filter(
			final double x ) {
		return (Double.isNaN(x)) ? 0 : x;
	}

}
