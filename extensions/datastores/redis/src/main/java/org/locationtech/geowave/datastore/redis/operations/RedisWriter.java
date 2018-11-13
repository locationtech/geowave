package org.locationtech.geowave.datastore.redis.operations;

import java.time.Instant;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedTimestampRow;
import org.locationtech.geowave.datastore.redis.util.RedisScoredSetWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisWriter implements
		RowWriter
{
	private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
	private final RedissonClient client;
	private final Compression compression;
	private final String setNamePrefix;
	private final LoadingCache<ByteArray, RedisScoredSetWrapper<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					partitionKey -> getSet(
							partitionKey.getBytes()));
	boolean isTimestampRequired;

	public RedisWriter(
			final RedissonClient client, final Compression compression,
			final String namespace,
			final String typeName,
			final String indexName,
			final boolean isTimestampRequired ) {
		this.client = client;
		this.compression = compression;
		setNamePrefix = RedisUtils
				.getRowSetPrefix(
						namespace,
						typeName,
						indexName);
		this.isTimestampRequired = isTimestampRequired;
	}

	private RedisScoredSetWrapper<GeoWaveRedisPersistedRow> getSet(
			final byte[] partitionKey ) {
		return RedisUtils
				.getRowSet(
						client,compression,
						setNamePrefix,
						partitionKey,
						isTimestampRequired);
	}

	@Override
	public void write(
			final GeoWaveRow[] rows ) {
		for (final GeoWaveRow row : rows) {
			write(
					row);
		}
	}

	@Override
	public void write(
			final GeoWaveRow row ) {
		ByteArray partitionKey;
		if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
			partitionKey = EMPTY_PARTITION_KEY;
		}
		else {
			partitionKey = new ByteArray(
					row.getPartitionKey());
		}
		for (final GeoWaveValue value : row.getFieldValues()) {
			setCache
					.get(
							partitionKey)
					.add(
							RedisUtils
									.getScore(
											row.getSortKey()),
							isTimestampRequired ? new GeoWaveRedisPersistedTimestampRow(
									(short) row.getNumberOfDuplicates(),
									row.getDataId(),
									value,
									Instant.now())
									: new GeoWaveRedisPersistedRow(
											(short) row.getNumberOfDuplicates(),
											row.getDataId(),
											value));

		}
	}

	@Override
	public void flush() {
		setCache.asMap().forEach((k,v) -> v.flush());
	}

	@Override
	public void close()
			throws Exception {
		for (RedisScoredSetWrapper<GeoWaveRedisPersistedRow> set : setCache.asMap().values()) {
			set.close();
		}
	}

}
