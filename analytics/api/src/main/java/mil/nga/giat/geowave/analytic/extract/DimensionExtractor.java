package mil.nga.giat.geowave.analytic.extract;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Strategy to extract a representative dimensions and Geometry for an Object
 * 
 * @param <T>
 */
public interface DimensionExtractor<T>
{
	/**
	 * 
	 * @param anObject
	 *            --
	 */
	public double[] getDimensions(
			T anObject );

	/**
	 * 
	 * @return Dimension names in the same order as dimentions returns from the
	 *         {@link DimensionExtractor#getDimensions(Object)}
	 */
	public String[] getDimensionNames();

	/**
	 * 
	 * @param anObject
	 *            -- an object with Geospatial properties
	 * @return A Point that must have the SRID set for a valid CRS.
	 */
	public Geometry getGeometry(
			T anObject );

	/**
	 * @param An
	 *            assigned group ID, if one exists. null, otherwisw. --
	 */
	public String getGroupID(
			T anObject );

}
