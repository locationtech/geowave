package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;

/**
 * This interface defines the set of statistics to capture for a specific
 * adapter.
 * 
 * @param <T>
 *            The type for the data elements that are being adapted by the
 *            adapter
 * 
 */
public interface StatisticsProvider<T>
{
	public ByteArrayId[] getSupportedStatisticsTypes();

	public DataStatistics<T> createDataStatistics(
			ByteArrayId statisticsId );

	public EntryVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId statisticsId );
}
