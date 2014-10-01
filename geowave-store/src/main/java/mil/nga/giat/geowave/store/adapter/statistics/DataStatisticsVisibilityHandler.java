package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.store.IngestEntryInfo;

public interface DataStatisticsVisibilityHandler<T>
{
	public byte[] getVisibility(
			IngestEntryInfo entryInfo,
			T entry );
}
