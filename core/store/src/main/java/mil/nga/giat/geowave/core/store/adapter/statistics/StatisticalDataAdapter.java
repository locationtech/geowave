package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

/**
 * This interface allows for a data adapter to define a set of statistics that
 * it would like to track
 * 
 * @param <T>
 *            The type for the data elements that are being adapted
 * 
 */
public interface StatisticalDataAdapter<T> extends
		WritableDataAdapter<T>
{
	public ByteArrayId[] getSupportedStatisticsIds();

	public DataStatistics<T> createDataStatistics(
			ByteArrayId statisticsId );

	public DataStatisticsVisibilityHandler<T> getVisibilityHandler(
			ByteArrayId statisticsId );
}
