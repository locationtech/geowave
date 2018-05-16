package mil.nga.giat.geowave.core.store.operations;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface Deleter extends
		AutoCloseable
{
	public void delete(
			GeoWaveRow row,
			DataAdapter<?> adapter );
}
