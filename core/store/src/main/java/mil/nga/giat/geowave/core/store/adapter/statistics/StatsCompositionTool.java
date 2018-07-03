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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.io.Closeable;
import java.io.Flushable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 *
 * This tool does not react to callbacks if the provided statistics store is
 * null or the provided data adapter does not implement
 * {@link DataStatisticsStore}.
 *
 * @param <T>
 *            Entry type
 */
public class StatsCompositionTool<T> implements
		IngestCallback<T>,
		ScanCallback<T, GeoWaveRow>,
		DeleteCallback<T, GeoWaveRow>,
		AutoCloseable,
		Closeable,
		Flushable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StatsCompositionTool.class);
	public static final int FLUSH_STATS_THRESHOLD = 16384;

	int updateCount = 0;
	DataStatisticsStore statisticsStore;
	List<DataStatisticsBuilder<T>> statisticsBuilders = null;
	final Object MUTEX = new Object();
	protected boolean skipFlush = false;

	public StatsCompositionTool(
			final DataStoreStatisticsProvider<T> statisticsProvider,
			final DataStatisticsStore statisticsStore,
			final PrimaryIndex index,
			final DataAdapter<T> adapter ) {
		this.statisticsStore = statisticsStore;
		this.init(
				index,
				adapter,
				statisticsProvider);
	}

	private void init(
			final PrimaryIndex index,
			final DataAdapter<T> adapter,
			final DataStoreStatisticsProvider<T> statisticsProvider ) {
		final ByteArrayId[] statisticsIds = statisticsProvider.getSupportedStatisticsTypes();
		statisticsBuilders = new ArrayList<DataStatisticsBuilder<T>>(
				statisticsIds.length);
		for (final ByteArrayId id : statisticsIds) {
			statisticsBuilders.add(new DataStatisticsBuilder<T>(
					index,
					adapter,
					statisticsProvider,
					id));
		}
		try {
			final Object v = System.getProperty("StatsCompositionTool.skipFlush");
			skipFlush = ((v != null) && v.toString().equalsIgnoreCase(
					"true"));
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Unable to determine property StatsCompositionTool.skipFlush",
					ex);
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (statisticsBuilders == null) {
			return;
		}
		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryDeleted(
						entry,
						kvs);
			}
			updateCount++;
			checkStats();
		}

	}

	@Override
	public void entryScanned(
			final T entry,
			final GeoWaveRow kv ) {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryScanned(
						entry,
						kv);
			}
			updateCount++;
			checkStats();
		}

	}

	/**
	 * Update statistics store
	 */
	@Override
	public void flush() {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				final Collection<DataStatistics<T>> statistics = builder.getStatistics();
				for (final DataStatistics<T> s : statistics) {
					// using a set and simply checking instanceof this is the
					// simplest approach to enable per partition statistics
					// within the current design

					// at some point stats should be re-designed to be simpler,
					// reducing the complexity of a stats builder wrapping a
					// statistic, which is wrapped by the composition tool,
					// which is used by the stats manager within a feature data
					// adapter etc.
					if (s instanceof DataStatisticsSet) {
						for (final DataStatistics<T> statInSet : ((DataStatisticsSet) s).getStatisticsSet()) {
							statisticsStore.incorporateStatistics(statInSet);
						}
					}
					else {
						statisticsStore.incorporateStatistics(s);
					}
				}
				statistics.clear();
			}
		}
	}

	/**
	 * Reset statistics, losing and updates since last flush
	 */
	public void reset() {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				final Collection<DataStatistics<T>> statistics = builder.getStatistics();
				statistics.clear();
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryIngested(
						entry,
						kvs);
			}
			updateCount++;
			checkStats();
		}
	}

	@Override
	public void close() {
		flush();
	}

	public void setStatisticsStore(
			final DataStatisticsStore statisticsStore ) {
		this.statisticsStore = statisticsStore;
	}

	private void checkStats() {
		if (!skipFlush && (updateCount > FLUSH_STATS_THRESHOLD)) {
			updateCount = 0;
			flush();
		}
	}

}
