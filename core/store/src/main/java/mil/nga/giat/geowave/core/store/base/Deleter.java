package mil.nga.giat.geowave.core.store.base;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

public interface Deleter<N> extends
		AutoCloseable
{
	public void delete(
			DataStoreEntryInfo entry,
			N nativeRow,
			DataAdapter<?> adapter );
}
