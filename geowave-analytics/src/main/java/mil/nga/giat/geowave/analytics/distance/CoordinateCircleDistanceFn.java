package mil.nga.giat.geowave.analytics.distance;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateCircleDistanceFn implements
		DistanceFn<Coordinate>
{

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		final double theta1 = Math.toRadians(x.y);
		final double theta2 = Math.toRadians(y.y);
		final double chg_theta = Math.toRadians(y.y - x.y);
		final double chg_lambda = Math.toRadians(y.x - x.x);

		final double a = (Math.sin(chg_theta / 2) * Math.sin(chg_theta / 2)) + (Math.cos(theta1) * Math.cos(theta2) * Math.sin(chg_lambda / 2) * Math.sin(chg_lambda / 2));
		final double c = 2 * Math.atan2(
				Math.sqrt(a),
				Math.sqrt(1 - a));

		return 6371.0 * c;
	}

}
