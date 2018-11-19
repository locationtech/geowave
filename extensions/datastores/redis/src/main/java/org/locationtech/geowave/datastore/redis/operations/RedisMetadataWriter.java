package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveTimestampMetadata;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;

public class RedisMetadataWriter implements
		MetadataWriter
{
	private final RScoredSortedSet<GeoWaveMetadata> set;
	private final boolean requiresTimestamp;

	public RedisMetadataWriter(
			final RScoredSortedSet<GeoWaveMetadata> set,
			final boolean requiresTimestamp ) {
		this.set = set;
		this.requiresTimestamp = requiresTimestamp;
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		set.add(
				RedisUtils.getScore(metadata.getPrimaryId()),
				requiresTimestamp ? new GeoWaveTimestampMetadata(
						metadata,
						System.currentTimeMillis()) : metadata);
	}

	@Override
	public void flush() {}

	@Override
	public void close()
			throws Exception {}
}
