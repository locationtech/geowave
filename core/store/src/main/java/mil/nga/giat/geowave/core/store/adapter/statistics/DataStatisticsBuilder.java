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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DataStatisticsBuilder<T> implements
		IngestCallback<T>,
		DeleteCallback<T, GeoWaveRow>,
		ScanCallback<T, GeoWaveRow>
{
	private final DataStoreStatisticsProvider<T> statisticsProvider;
	private final Map<ByteArrayId, DataStatistics<T>> statisticsMap = new HashMap<ByteArrayId, DataStatistics<T>>();
	private final ByteArrayId statisticsId;
	private final EntryVisibilityHandler<T> visibilityHandler;

	public DataStatisticsBuilder(
			final PrimaryIndex index,
			final DataAdapter<T> adapter,
			final DataStoreStatisticsProvider<T> statisticsProvider,
			final ByteArrayId statisticsId ) {
		this.statisticsProvider = statisticsProvider;
		this.statisticsId = statisticsId;
		this.visibilityHandler = statisticsProvider.getVisibilityHandler(
				index.getIndexModel(),
				adapter,
				statisticsId);
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entry,
						kvs));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			if (statistics == null) {
				return;
			}
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entry,
				kvs);
	}

	public Collection<DataStatistics<T>> getStatistics() {
		return statisticsMap.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kv ) {
		final ByteArrayId visibilityByteArray = new ByteArrayId(
				visibilityHandler.getVisibility(
						entry,
						kv));
		DataStatistics<T> statistics = statisticsMap.get(visibilityByteArray);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			statistics.setVisibility(visibilityByteArray.getBytes());
			statisticsMap.put(
					visibilityByteArray,
					statistics);
		}
		if (statistics instanceof DeleteCallback) {
			((DeleteCallback<T, GeoWaveRow>) statistics).entryDeleted(
					entry,
					kv);
		}
	}

	@Override
	public void entryScanned(
			final T entry,
			final GeoWaveRow kv ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entry,
						kv));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			if (statistics == null) {
				return;
			}
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entry,
				kv);

	}
}
