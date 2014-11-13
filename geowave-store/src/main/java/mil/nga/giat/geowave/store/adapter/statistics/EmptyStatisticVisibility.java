package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.store.IngestEntryInfo;

/**
 * 
 * Supplies not additional visibility
 *
 * @param <T>
 */
public class EmptyStatisticVisibility<T> implements
		DataStatisticsVisibilityHandler<T> {

	@Override
	public byte[] getVisibility(
			final IngestEntryInfo entryInfo,
			final T entry ) {
		return null;
	}

}
