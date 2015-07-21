package mil.nga.giat.geowave.analytic.distance;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
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

	private static final double EPSILON = 0.0000000001;

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
		catch (TransformException e) {
			throw new RuntimeException(
					"Failed to transform coordinates to provided CRS",
					e);
		}
		catch (java.lang.AssertionError ae) {
			// wierd error with orthodromic distance
			if ((Math.abs(c1.x - c2.x) < EPSILON) && (Math.abs(c1.y - c2.y) < EPSILON)) return 0.0;
			throw ae;
		}

	}

	protected CoordinateReferenceSystem getCRS() {
		return DEFAULT_CRS;
	}

}
