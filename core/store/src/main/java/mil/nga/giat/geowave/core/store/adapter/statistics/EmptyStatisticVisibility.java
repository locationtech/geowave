package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

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
			final T entry,
			final GeoWaveRow... kvs ) {
		return new byte[0];
	}

}
