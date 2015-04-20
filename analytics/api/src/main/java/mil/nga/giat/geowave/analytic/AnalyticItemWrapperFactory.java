package mil.nga.giat.geowave.analytic;

import java.io.IOException;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Create an analytic item wrapper for the provided item.
 * 
 * 
 * @param <T>
 */
public interface AnalyticItemWrapperFactory<T>
{
	/**
	 * Wrap the item.
	 * 
	 * @param item
	 * @return
	 */
	public AnalyticItemWrapper<T> create(
			T item );

	/**
	 * Creates a new item based on the old item with new coordinates and
	 * dimension values
	 * 
	 * @param feature
	 * @param coordinate
	 * @param extraNames
	 * @param extraValues
	 * @return
	 */
	public AnalyticItemWrapper<T> createNextItem(
			final T feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues );

	public void initialize(
			final ConfigurationWrapper context )
			throws IOException;
}
