package mil.nga.giat.geowave.core.store.operations;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;

public interface MetadataWriter extends
		AutoCloseable
{
	public void write(
			GeoWaveMetadata metadata );

	public void flush();
}
