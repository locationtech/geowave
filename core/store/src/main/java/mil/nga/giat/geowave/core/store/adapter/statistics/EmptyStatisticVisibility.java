package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;

/**
 * 
 * Supplies not additional visibility
 * 
 * @param <T>
 */
public class EmptyStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	@Override
	public byte[] getVisibility(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		return new byte[0];
	}

}
