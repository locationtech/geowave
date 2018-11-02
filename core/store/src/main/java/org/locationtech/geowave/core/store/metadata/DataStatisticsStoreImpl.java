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

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;

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
		AbstractGeoWavePersistence<InternalDataStatistics<?, ?, ?>> implements
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
		final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(
				MAX_ENTRIES).expireAfterWrite(
				STATISTICS_CACHE_TIMEOUT,
				TimeUnit.MILLISECONDS);
		cache = cacheBuilder.<ByteArray, Map<String, InternalDataStatistics<?, ?, ?>>> build();
	}

	private String getCombinedAuths(
			final String[] authorizations ) {
		final StringBuilder sb = new StringBuilder();
		if (authorizations != null) {
			Arrays.sort(authorizations);
			for (int i = 0; i < authorizations.length; i++) {
				sb.append(authorizations[i]);
				if ((i + 1) < authorizations.length) {
					sb.append("&");
				}
			}
		}
		return sb.toString();
	}

	@Override
	public void incorporateStatistics(
			final InternalDataStatistics<?, ?, ?> statistics ) {
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
			final ByteArray primaryId,
			final ByteArray secondaryId,
			final InternalDataStatistics<?, ?, ?> object,
			final String... authorizations ) {
		final ByteArray combinedId = getCombinedId(
				primaryId,
				secondaryId);

		Map<String, InternalDataStatistics<?, ?, ?>> cached = (Map<String, InternalDataStatistics<?, ?, ?>>) cache
				.getIfPresent(combinedId);
		if (cached == null) {
			cached = new ConcurrentHashMap<>();
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
			final ByteArray primaryId,
			final ByteArray secondaryId,
			final String... authorizations ) {
		final ByteArray combinedId = getCombinedId(
				primaryId,
				secondaryId);
		final Map<String, InternalDataStatistics<?, ?, ?>> cached = (Map<String, InternalDataStatistics<?, ?, ?>>) cache
				.getIfPresent(combinedId);
		if (cached != null) {
			return cached.get(getCombinedAuths(authorizations));
		}
		return null;
	}

	protected ByteArray shortToByteArrayId(
			final short internalAdapterId ) {
		return new ByteArray(
				ByteArrayUtils.shortToByteArray(internalAdapterId));
	}

	protected short byteArrayToShort(
			final byte[] bytes ) {
		return ByteArrayUtils.byteArrayToShort(bytes);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getDataStatistics(
			final short adapterId,
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return internalGetDataStatistics(
				adapterId,
				statisticsType,
				authorizations);
	}

	protected CloseableIterator<InternalDataStatistics<?, ?, ?>> internalGetDataStatistics(
			final Short adapterId,
			final ByteArray primaryId,
			final String... authorizations ) {

		final ByteArray secondaryId = adapterId == null ? null : shortToByteArrayId(adapterId);
		final Object cacheResult = getObjectFromCache(
				primaryId,
				secondaryId,
				authorizations);

		// if there's an exact match in the cache return a singleton
		if (cacheResult != null) {
			return new CloseableIterator.Wrapper<>(
					Iterators.singletonIterator((InternalDataStatistics<?, ?, ?>) cacheResult));
		}

		// otherwise scan
		// TODO issue 1443 will enable prefix scans on the primary ID
		return internalGetObjects(new MetadataQuery(
				primaryId.getBytes(),
				secondaryId == null ? null : secondaryId.getBytes(),
				authorizations));
	}

	@Override
	protected InternalDataStatistics<?, ?, ?> entryToValue(
			final GeoWaveMetadata entry,
			final String... authorizations ) {
		final InternalDataStatistics<?, ?, ?> stats = super.entryToValue(
				entry,
				authorizations);
		if (stats != null) {
			return setFields(
					entry,
					stats,
					byteArrayToShort(entry.getSecondaryId()));
		}
		return null;
	}

	public static InternalDataStatistics<?, ?, ?> setFields(
			final GeoWaveMetadata entry,
			final InternalDataStatistics<?, ?, ?> basicStats,
			final short adapterId ) {
		if (basicStats != null) {
			basicStats.setAdapterId(adapterId);
			final int index = Bytes.indexOf(
					entry.getPrimaryId(),
					(byte) 0);
			if ((index > 0) && (index < (entry.getPrimaryId().length - 1))) {
				basicStats.setType(new BaseStatisticsType(
						Arrays.copyOfRange(
								entry.getPrimaryId(),
								0,
								index)));

				basicStats.setExtendedId(StringUtils.stringFromBinary(Arrays.copyOfRange(
						entry.getPrimaryId(),
						index + 1,
						entry.getPrimaryId().length)));
			}
			else {
				basicStats.setType(new BaseStatisticsType(
						entry.getPrimaryId()));
			}
			final byte[] visibility = entry.getVisibility();
			if (visibility != null) {
				basicStats.setVisibility(visibility);
			}
		}
		return basicStats;
	}

	@Override
	protected ByteArray getPrimaryId(
			final InternalDataStatistics<?, ?, ?> persistedObject ) {
		return getPrimaryId(
				persistedObject.getType(),
				persistedObject.getExtendedId());
	}

	public static ByteArray getPrimaryId(
			final StatisticsType<?, ?> type,
			final String extendedId ) {
		if ((extendedId != null) && (extendedId.length() > 0)) {
			return new ByteArray(
					Bytes.concat(
							type.getBytes(),
							new byte[] {
								(byte) 0
							},
							StringUtils.stringToBinary(extendedId)));
		}
		return type;
	}

	@Override
	protected ByteArray getSecondaryId(
			final InternalDataStatistics<?, ?, ?> persistedObject ) {
		return shortToByteArrayId(persistedObject.getAdapterId());
	}

	@Override
	public void setStatistics(
			final InternalDataStatistics<?, ?, ?> statistics ) {
		removeStatistics(
				statistics.getAdapterId(),
				statistics.getType());
		addObject(statistics);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getAllDataStatistics(
			final String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public boolean removeStatistics(
			final short adapterId,
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return deleteObject(
				statisticsType,
				shortToByteArrayId(adapterId),
				authorizations);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getDataStatistics(
			final short adapterId,
			final String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				shortToByteArrayId(adapterId),
				authorizations);
	}

	@Override
	protected byte[] getVisibility(
			final InternalDataStatistics<?, ?, ?> entry ) {
		return entry.getVisibility();
	}

	@Override
	public void removeAllStatistics(
			final short adapterId,
			final String... authorizations ) {
		deleteObjects(
				shortToByteArrayId(adapterId),
				authorizations);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getDataStatistics(
			final short adapterId,
			final String extendedId,
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return internalGetDataStatistics(
				adapterId,
				getPrimaryId(
						statisticsType,
						extendedId),
				authorizations);
	}

	@Override
	public boolean removeStatistics(
			final short adapterId,
			final String extendedId,
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return deleteObject(
				getPrimaryId(
						statisticsType,
						extendedId),
				shortToByteArrayId(adapterId),
				authorizations);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getDataStatistics(
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return internalGetDataStatistics(
				null,
				statisticsType,
				authorizations);
	}

	@Override
	public CloseableIterator<InternalDataStatistics<?, ?, ?>> getDataStatistics(
			final String extendedIdPrefix,
			final StatisticsType<?, ?> statisticsType,
			final String... authorizations ) {
		return internalGetDataStatistics(
				null,
				getPrimaryId(
						statisticsType,
						extendedIdPrefix),
				authorizations);
	}
}
