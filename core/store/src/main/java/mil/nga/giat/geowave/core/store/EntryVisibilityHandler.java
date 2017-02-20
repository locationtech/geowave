package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface EntryVisibilityHandler<T>
{
	public byte[] getVisibility(
			T entry,
			GeoWaveRow... kvs );
}
