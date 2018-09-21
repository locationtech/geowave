/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;

import com.google.common.cache.CacheBuilder;

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
		AbstractGeoWavePersistence<InternalDataStatistics<?>> implements
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
		this.cache = cacheBuilder.<ByteArrayId, Map<String, InternalDataStatistics<?>>> build();
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
			final InternalDataStatistics<?> statistics ) {
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
			final InternalDataStatistics<?> object,
			final String... authorizations ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);

		Map<String, InternalDataStatistics<?>> cached = (Map<String, InternalDataStatistics<?>>) cache
				.getIfPresent(combinedId);
		if (cached == null) {
			cached = new ConcurrentHashMap<String, InternalDataStatistics<?>>();
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
		Map<String, InternalDataStatistics<?>> cached = (Map<String, InternalDataStatistics<?>>) cache
				.getIfPresent(combinedId);
		if (cached != null) {
			return cached.get(getCombinedAuths(authorizations));
		}
		return null;
	}

	protected ByteArrayId shortToByteArrayId(
			short internalAdapterId ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToByteArray(internalAdapterId));
	}

	protected short byteArrayToShort(
			byte[] bytes ) {
		return ByteArrayUtils.byteArrayToShort(bytes);
	}

	@Override
	public InternalDataStatistics<?> getDataStatistics(
			final short internalAdapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return internalGetObject(
				statisticsId,
				shortToByteArrayId(internalAdapterId),
				// for data statistics we don't want to log if its not found
				false,
				authorizations);
	}

	@Override
	protected InternalDataStatistics<?> entryToValue(
			final GeoWaveMetadata entry,
			String... authorizations ) {
		final InternalDataStatistics<?> stats = super.entryToValue(
				entry,
				authorizations);
		if (stats != null) {
			stats.setInternalDataAdapterId(byteArrayToShort(entry.getSecondaryId()));
			stats.setStatisticsType(new ByteArrayId(
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
			final InternalDataStatistics<?> persistedObject ) {
		return persistedObject.getStatisticsType();
	}

	@Override
	protected ByteArrayId getSecondaryId(
			final InternalDataStatistics<?> persistedObject ) {
		return shortToByteArrayId(persistedObject.getInternalDataAdapterId());
	}

	@Override
	public void setStatistics(
			final InternalDataStatistics<?> statistics ) {
		removeStatistics(
				statistics.getInternalDataAdapterId(),
				statistics.getStatisticsType());
		addObject(statistics);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?>> getAllDataStatistics(
			final String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public boolean removeStatistics(
			final short internalAdapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return deleteObject(
				statisticsId,
				shortToByteArrayId(internalAdapterId),
				authorizations);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?>> getDataStatistics(
			final short internalAdapterId,
			final String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				shortToByteArrayId(internalAdapterId),
				authorizations);
	}

	@Override
	protected byte[] getVisibility(
			final InternalDataStatistics<?> entry ) {
		return entry.getVisibility();
	}

	@Override
	public void removeAllStatistics(
			final short internalAdapterId,
			final String... authorizations ) {
		deleteObjects(
				shortToByteArrayId(internalAdapterId),
				authorizations);
	}
}
