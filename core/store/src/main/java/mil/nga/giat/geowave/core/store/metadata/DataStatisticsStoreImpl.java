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

	public DataStatisticsStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.STATS);
	}

	@Override
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		// because we're using the combiner, we should simply be able to add the
		// object
		addObject(statistics);

		// TODO if we do allow caching after we add a statistic to Accumulo we
		// do need to make sure we update our cache, but for now we aren't using
		// the cache at all

	}

	@Override
	protected void addObjectToCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final DataStatistics<?> object ) {
		// don't use the cache at all for now

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Accumulo tables
	}

	@Override
	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Accumulo tables
		return null;
	}

	@Override
	protected boolean deleteObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Accumulo tables
		return true;
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
			final GeoWaveMetadata entry ) {
		final DataStatistics<?> stats = super.entryToValue(entry);
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
