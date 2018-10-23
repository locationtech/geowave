package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.redisson.api.RScoredSortedSet;

public class RedisMetadataDeleter implements
		MetadataDeleter
{
	private final RScoredSortedSet<GeoWaveMetadata> set;
	private final MetadataType metadataType;

	public RedisMetadataDeleter(
			final RScoredSortedSet<GeoWaveMetadata> set,
			final MetadataType metadataType ) {
		this.set = set;
		this.metadataType = metadataType;
	}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		boolean atLeastOneDeletion = false;

		boolean noFailures = true;
		try (CloseableIterator<GeoWaveMetadata> it = new RedisMetadataReader(
				set,
				metadataType).query(
				query,
				false)) {
			while (it.hasNext()) {
				if (set.remove(it.next())) {
					atLeastOneDeletion = true;
				}
				else {
					noFailures = false;
				}
			}
		}
		return atLeastOneDeletion && noFailures;
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}

}
