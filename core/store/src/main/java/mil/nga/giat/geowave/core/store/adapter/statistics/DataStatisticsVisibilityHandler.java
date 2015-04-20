package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;

public interface DataStatisticsVisibilityHandler<T>
{
	public byte[] getVisibility(
			DataStoreEntryInfo entryInfo,
			T entry );
}
