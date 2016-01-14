package mil.nga.giat.geowave.analytic.distance;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.geotools.referencing.datum.DefaultEllipsoid;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateCircleDistanceFn implements
		DistanceFn<Coordinate>
{

	/**
	 *
	 */
	private static final long serialVersionUID = -1245559892132762143L;
	protected static final CoordinateReferenceSystem DEFAULT_CRS;

	static {
		try {
			DEFAULT_CRS = CRS.decode(
					"EPSG:4326",
					true);
		}
		catch (final FactoryException e) {
			throw new RuntimeException(
					"Failed to load default EPSG:4326 coordinate reference system",
					e);
		}
	}

	@Override
	public double measure(
			final Coordinate c1,
			final Coordinate c2 ) {
		try {
			return JTS.orthodromicDistance(
					c1,
					c2,
					getCRS());
		}
		catch (final TransformException e) {
			throw new RuntimeException(
					"Failed to transform coordinates to provided CRS",
					e);
		}
		catch (final java.lang.AssertionError ae) {
			// weird error with orthodromic distance..when distance is too close
			// (0.05 meter), it fails the tolerance test

			final GeodeticCalculator calc = new GeodeticCalculator(
					getCRS());
			calc.setStartingGeographicPoint(
					c1.x,
					c1.y);
			calc.setDestinationGeographicPoint(
					c2.x,
					c2.y);
			return ((DefaultEllipsoid) calc.getEllipsoid()).orthodromicDistance(
					calc.getStartingGeographicPoint().getX(),
					calc.getStartingGeographicPoint().getY(),
					calc.getDestinationGeographicPoint().getX(),
					calc.getDestinationGeographicPoint().getY());
		}
	}

	protected CoordinateReferenceSystem getCRS() {
		return DEFAULT_CRS;
	}

}
