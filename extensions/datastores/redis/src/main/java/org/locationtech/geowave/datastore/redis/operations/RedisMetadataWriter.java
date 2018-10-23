package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;

public class RedisMetadataWriter implements
		MetadataWriter
{
	private RScoredSortedSet<GeoWaveMetadata> set;

	public RedisMetadataWriter(
			RScoredSortedSet<GeoWaveMetadata> set ) {
		this.set = set;
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		set.add(
				RedisUtils.getScore(metadata.getPrimaryId()),
				metadata);
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}
}
