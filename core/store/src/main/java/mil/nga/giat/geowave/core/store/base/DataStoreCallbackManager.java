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
package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.DeleteCallbackList;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataAdapter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataManager;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class DataStoreCallbackManager
{

	final private DataStatisticsStore statsStore;
	private boolean persistStats = true;
	final private SecondaryIndexDataStore secondaryIndexStore;

	final private boolean captureAdapterStats;

	final Map<Short, IngestCallback<?>> icache = new HashMap<Short, IngestCallback<?>>();
	final Map<Short, DeleteCallback<?, GeoWaveRow>> dcache = new HashMap<Short, DeleteCallback<?, GeoWaveRow>>();

	public DataStoreCallbackManager(
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexStore,
			boolean captureAdapterStats ) {
		this.statsStore = statsStore;
		this.secondaryIndexStore = secondaryIndexStore;
		this.captureAdapterStats = captureAdapterStats;
	}

	public <T> IngestCallback<T> getIngestCallback(
			final InternalDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!icache.containsKey(writableAdapter.getInternalAdapterId())) {
			final DataStoreStatisticsProvider<T> statsProvider = new DataStoreStatisticsProvider<T>(
					writableAdapter,
					index,
					captureAdapterStats);
			final List<IngestCallback<T>> callbackList = new ArrayList<IngestCallback<T>>();
			if ((writableAdapter.getAdapter() instanceof StatisticsProvider) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsProvider,
						statsStore,
						index,
						(DataAdapter<T>) writableAdapter.getAdapter()));
			}
			if (captureAdapterStats && writableAdapter.getAdapter() instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter.getAdapter(),
						index));
			}
			icache.put(
					writableAdapter.getInternalAdapterId(),
					new IngestCallbackList<T>(
							callbackList));
		}
		return (IngestCallback<T>) icache.get(writableAdapter.getInternalAdapterId());

	}

	public void setPersistStats(
			final boolean persistStats ) {
		this.persistStats = persistStats;
	}

	public <T> DeleteCallback<T, GeoWaveRow> getDeleteCallback(
			final InternalDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!dcache.containsKey(writableAdapter.getInternalAdapterId())) {
			final DataStoreStatisticsProvider<T> statsProvider = new DataStoreStatisticsProvider<T>(
					writableAdapter,
					index,
					captureAdapterStats);
			final List<DeleteCallback<T, GeoWaveRow>> callbackList = new ArrayList<DeleteCallback<T, GeoWaveRow>>();
			if ((writableAdapter.getAdapter() instanceof StatisticsProvider) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsProvider,
						statsStore,
						index,
						(DataAdapter<T>) writableAdapter.getAdapter()));
			}
			if (captureAdapterStats && writableAdapter.getAdapter() instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter.getAdapter(),
						index));
			}
			dcache.put(
					writableAdapter.getInternalAdapterId(),
					new DeleteCallbackList<T, GeoWaveRow>(
							callbackList));
		}
		return (DeleteCallback<T, GeoWaveRow>) dcache.get(writableAdapter.getInternalAdapterId());

	}

	public void close()
			throws IOException {
		for (final IngestCallback<?> callback : icache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
		for (final DeleteCallback<?, GeoWaveRow> callback : dcache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}
}
