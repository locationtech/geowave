package mil.nga.giat.geowave.core.store.base;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

public interface Deleter extends
		AutoCloseable
{
	public void delete(
			DataStoreEntryInfo entry,
			DataAdapter<?> adapter );
}
