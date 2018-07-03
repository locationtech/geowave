/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Map;

import com.google.common.cache.CacheBuilder;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "INDEX" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 *
 **/
public class DataStatisticsStoreImpl extends
		AbstractGeoWavePersistence<DataStatistics<?>> implements
		DataStatisticsStore
{
	// this is fairly arbitrary at the moment because it is the only custom
	// server op added
	public static final int STATS_COMBINER_PRIORITY = 10;
	public static final String STATISTICS_COMBINER_NAME = "STATS_COMBINER";

	private static final long STATISTICS_CACHE_TIMEOUT = 60 * 1000; // 1 Minute

	public DataStatisticsStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.STATS);
	}

	@Override
	protected void buildCache() {
		CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(
				MAX_ENTRIES).expireAfterWrite(
				STATISTICS_CACHE_TIMEOUT,
				TimeUnit.MILLISECONDS);
		this.cache = cacheBuilder.<ByteArrayId, Map<String, DataStatistics<?>>> build();
	}

	private String getCombinedAuths(
			String[] authorizations ) {
		StringBuilder sb = new StringBuilder();
		if (authorizations != null) {
			Arrays.sort(authorizations);
			for (int i = 0; i < authorizations.length; i++) {
				sb.append(authorizations[i]);
				if (i + 1 < authorizations.length) {
					sb.append("&");
				}
			}
		}
		return sb.toString();
	}

	@Override
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		// because we're using the combiner, we should simply be able to add the
		// object
		addObject(statistics);
		deleteObjectFromCache(
				getPrimaryId(statistics),
				getSecondaryId(statistics));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void addObjectToCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final DataStatistics<?> object,
			final String... authorizations ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);

		Map<String, DataStatistics<?>> cached = (Map<String, DataStatistics<?>>) cache.getIfPresent(combinedId);
		if (cached == null) {
			cached = new ConcurrentHashMap<String, DataStatistics<?>>();
			cache.put(
					combinedId,
					cached);
		}
		cached.put(
				getCombinedAuths(authorizations),
				object);

	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		Map<String, DataStatistics<?>> cached = (Map<String, DataStatistics<?>>) cache.getIfPresent(combinedId);
		if (cached != null) {
			return cached.get(getCombinedAuths(authorizations));
		}
		return null;
	}

	@Override
	public DataStatistics<?> getDataStatistics(
			final short internalAdapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return internalGetObject(
				statisticsId,
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				// for data statistics we don't want to log if its not found
				false,
				authorizations);
	}

	@Override
	protected DataStatistics<?> entryToValue(
			final GeoWaveMetadata entry,
			String... authorizations ) {
		final DataStatistics<?> stats = super.entryToValue(
				entry,
				authorizations);
		if (stats != null) {
			stats.setInternalDataAdapterId(ByteArrayUtils.byteArrayToShort(entry.getSecondaryId()));
			stats.setStatisticsId(new ByteArrayId(
					entry.getPrimaryId()));
			final byte[] visibility = entry.getVisibility();
			if (visibility != null) {
				stats.setVisibility(visibility);
			}
		}
		return stats;
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getStatisticsId();
	}

	@Override
	protected ByteArrayId getSecondaryId(
			final DataStatistics<?> persistedObject ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToByteArray(persistedObject.getInternalDataAdapterId()));
	}

	@Override
	public void setStatistics(
			final DataStatistics<?> statistics ) {
		removeStatistics(
				statistics.getInternalDataAdapterId(),
				statistics.getStatisticsId());
		addObject(statistics);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			final String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public boolean removeStatistics(
			final short adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return deleteObject(
				statisticsId,
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(adapterId)),
				authorizations);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			final short adapterId,
			final String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(adapterId)),
				authorizations);
	}

	@Override
	protected byte[] getVisibility(
			final DataStatistics<?> entry ) {
		return entry.getVisibility();
	}

	@Override
	public void removeAllStatistics(
			final short adapterId,
			final String... authorizations ) {
		deleteObjects(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(adapterId)),
				authorizations);
	}
}
