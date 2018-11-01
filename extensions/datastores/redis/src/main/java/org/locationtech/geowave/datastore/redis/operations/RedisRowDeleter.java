package org.locationtech.geowave.datastore.redis.operations;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedisRowDeleter implements
		RowDeleter
{

	private final LoadingCache<Pair<String, Short>, RScoredSortedSet<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					nameAndAdapterId -> getSet(
							nameAndAdapterId));
	private final RedissonClient client;
	private final PersistentAdapterStore adapterStore;
	private final InternalAdapterStore internalAdapterStore;
	private final String indexName;
	private final String namespace;

	public RedisRowDeleter(
			final RedissonClient client,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final String indexName,
			final String namespace ) {
		this.client = client;
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
		this.indexName = indexName;
		this.namespace = namespace;
	}

	@Override
	public void close()
			throws Exception {}

	private RScoredSortedSet<GeoWaveRedisPersistedRow> getSet(
			final Pair<String, Short> setNameAndAdapterId ) {
		return RedisUtils
				.getRowSet(
						client,
						setNameAndAdapterId.getLeft(),
						RedisUtils
								.isSortByTime(
										adapterStore
												.getAdapter(
														setNameAndAdapterId.getRight())));
	}

	@Override
	public void delete(
			final GeoWaveRow row ) {
		final RScoredSortedSet<GeoWaveRedisPersistedRow> set = setCache
				.get(
						Pair
								.of(
										RedisUtils
												.getRowSetName(
														namespace,
														internalAdapterStore
																.getTypeName(
																		row.getAdapterId()),
														indexName,
														row.getPartitionKey()),
										row.getAdapterId()));
		Arrays
				.stream(
						((GeoWaveRedisRow) row).getPersistedRows())
				.forEach(
						r -> set
								.remove(
										r));
	}

	@Override
	public void flush() {}

}
