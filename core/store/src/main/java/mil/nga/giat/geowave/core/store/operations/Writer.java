package mil.nga.giat.geowave.core.store.operations;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface is returned by DataStoreOperations and useful for general
 * purpose writing of entries. The default implementation of AccumuloOperations
 * will wrap this interface with a BatchWriter but can be overridden for other
 * mechanisms to write the data.
 */
public interface Writer extends
		AutoCloseable
{
	public void write(
			GeoWaveRow[] rows );

	public void write(
			GeoWaveRow row );

	public void flush();
}
