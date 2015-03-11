package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.store.DataStoreEntryInfo;

public interface DataStatisticsVisibilityHandler<T>
{
	public byte[] getVisibility(
			DataStoreEntryInfo entryInfo,
			T entry );
}
