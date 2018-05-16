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
package mil.nga.giat.geowave.core.store.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;

/**
 * This is responsible for persisting data statistics (either in memory or to
 * disk depending on the implementation).
 */
public class OldMemoryDataStatisticsStore implements
		DataStatisticsStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(OldMemoryDataStatisticsStore.class);
	private final Map<Key, DataStatistics<?>> statsMap = new HashMap<Key, DataStatistics<?>>();

	/**
	 * This will write the statistics to the underlying store. Note that this
	 * will overwrite whatever the current persisted statistics are with the
	 * given statistics ID and data adapter ID. Use incorporateStatistics to
	 * aggregate the statistics with any existing statistics.
	 *
	 * @param statistics
	 *            The statistics to write
	 *
	 */
	@Override
	public void setStatistics(
			final DataStatistics<?> statistics ) {
		statsMap.put(
				new Key(
						statistics.getDataAdapterId(),
						statistics.getStatisticsId(),
						statistics.getVisibility()),
				statistics);
	}

	/**
	 * Add the statistics to the store, overwriting existing data statistics
	 * with the aggregation of these statistics and the existing statistics
	 *
	 * @param statistics
	 *            the data statistics
	 */
	@Override
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		final Key key = new Key(
				statistics.getDataAdapterId(),
				statistics.getStatisticsId(),
				statistics.getVisibility());
		DataStatistics<?> existingStats = statsMap.get(key);
		if (existingStats == null) {
			statsMap.put(
					key,
					statistics);
		}
		else {
			existingStats = (DataStatistics<?>) PersistenceUtils.fromBinary(PersistenceUtils.toBinary(existingStats));
			existingStats.merge(statistics);
			statsMap.put(
					key,
					existingStats);
		}
	}

	/**
	 * Get all data statistics from the store by a data adapter ID
	 *
	 * @param adapterId
	 *            the data adapter ID
	 * @return the list of statistics for the given adapter, empty if it doesn't
	 *         exist
	 */
	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		final List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (final DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId)) {
				statSet.add(stat);
			}

		}
		return new CloseableIterator.Wrapper<DataStatistics<?>>(
				statSet.iterator());
	}

	/**
	 * Get all data statistics from the store
	 *
	 * @return the list of all statistics
	 */
	@Override
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			final String... authorizations ) {
		return new CloseableIterator.Wrapper<DataStatistics<?>>(
				statsMap.values().iterator());
	}

	/**
	 * Get statistics by adapter ID and the statistics ID (which will define a
	 * unique statistic)
	 *
	 * @param adapterId
	 *            The adapter ID for the requested statistics
	 * @param statisticsId
	 *            the statistics ID for the requested statistics
	 * @return the persisted statistics value
	 */
	@Override
	public DataStatistics<?> getDataStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {

		final List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (final DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId) && stat.getStatisticsId().equals(
					statisticsId) && MemoryStoreUtils.isAuthorized(
					stat.getVisibility(),
					authorizations)) {
				statSet.add(stat);
			}

		}

		return (statSet.size()) > 0 ? statSet.get(0) : null;
	}

	/**
	 * Remove a statistic from the store
	 *
	 * @param adapterId
	 * @param statisticsId
	 * @return a flag indicating whether a statistic had existed with the given
	 *         IDs and was successfully deleted.
	 */
	@Override
	public boolean removeStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		final List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (final DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId) && stat.getStatisticsId().equals(
					statisticsId) && MemoryStoreUtils.isAuthorized(
					stat.getVisibility(),
					authorizations)) {
				statSet.add(stat);
			}

		}
		if (statSet.size() > 0) {
			final DataStatistics<?> statistics = statSet.get(0);
			statsMap.remove(new Key(
					statistics.getDataAdapterId(),
					statistics.getStatisticsId(),
					statistics.getVisibility()));
			return true;
		}
		return false;

	}

	@Override
	public void removeAll() {
		statsMap.clear();
	}

	private static class Key
	{
		ByteArrayId adapterId;
		ByteArrayId statisticsId;
		byte[] authorizations;

		public Key(
				final ByteArrayId adapterId,
				final ByteArrayId statisticsId,
				final byte[] authorizations ) {
			super();
			this.adapterId = adapterId;
			this.statisticsId = statisticsId;
			this.authorizations = authorizations;
		}

		public ByteArrayId getAdapterId() {
			return adapterId;
		}

		public void setAdapterId(
				final ByteArrayId adapterId ) {
			this.adapterId = adapterId;
		}

		public ByteArrayId getStatisticsId() {
			return statisticsId;
		}

		public void setStatisticsId(
				final ByteArrayId statisticsId ) {
			this.statisticsId = statisticsId;
		}

		public byte[] getAuthorizations() {
			return authorizations;
		}

		public void setAuthorizations(
				final byte[] authorizations ) {
			this.authorizations = authorizations;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
			result = (prime * result) + Arrays.hashCode(authorizations);
			result = (prime * result) + ((statisticsId == null) ? 0 : statisticsId.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final Key other = (Key) obj;
			if (adapterId == null) {
				if (other.adapterId != null) {
					return false;
				}
			}
			else if (!adapterId.equals(other.adapterId)) {
				return false;
			}
			if (!Arrays.equals(
					authorizations,
					other.authorizations)) {
				return false;
			}
			if (statisticsId == null) {
				if (other.statisticsId != null) {
					return false;
				}
			}
			else if (!statisticsId.equals(other.statisticsId)) {
				return false;
			}
			return true;
		}

	}

	@Override
	public void removeAllStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		final Iterator<Entry<Key, DataStatistics<?>>> it = statsMap.entrySet().iterator();
		while (it.hasNext()) {
			final Entry<Key, DataStatistics<?>> entry = it.next();
			if (entry.getKey().adapterId.equals(adapterId) && MemoryStoreUtils.isAuthorized(
					entry.getKey().authorizations,
					authorizations)) {
				it.remove();
			}
		}
	}
}
