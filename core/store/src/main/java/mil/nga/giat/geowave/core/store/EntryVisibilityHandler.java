package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public interface EntryVisibilityHandler<T>
{
	public byte[] getVisibility(
			DataStoreEntryInfo entryInfo,
			T entry );
}
