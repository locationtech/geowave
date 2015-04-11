package mil.nga.giat.geowave.analytics.distance;

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

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		try {
			return JTS.orthodromicDistance(
					x,
					y,
					getCRS());
		}
		catch (TransformException e) {
			throw new RuntimeException(
					"Failed to transform coordinates to provided CRS",
					e);
		}

	}

	protected CoordinateReferenceSystem getCRS() {
		return DEFAULT_CRS;
	}

}
