package mil.nga.giat.geowave.core.store.operations;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;

public interface MetadataReader
{
	public CloseableIterator<GeoWaveMetadata> query(
			MetadataQuery query );
}
