package mil.nga.giat.geowave.analytic.extract;

import com.vividsolutions.jts.geom.Point;

/**
 * Strategy to extract a representative centroid from some Geospatial object
 * 
 * @param <T>
 */
public interface CentroidExtractor<T>
{
	/**
	 * 
	 * @param anObject
	 *            -- an object with Geospatial properties
	 * @return A Point that must have the SRID set for a valid CRS.
	 */
	public Point getCentroid(
			T anObject );

}
