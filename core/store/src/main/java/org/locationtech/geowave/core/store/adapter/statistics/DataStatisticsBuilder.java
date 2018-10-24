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
package org.locationtech.geowave.core.store.adapter.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.DataStoreStatisticsProvider;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class DataStatisticsBuilder<T, R, B extends StatisticsQueryBuilder<R, B>> implements
		IngestCallback<T>,
		DeleteCallback<T, GeoWaveRow>,
		ScanCallback<T, GeoWaveRow>
{
	private final DataStoreStatisticsProvider<T> statisticsProvider;
	private final Map<ByteArray, InternalDataStatistics<T, R, B>> statisticsMap = new HashMap<>();
	private final StatisticsId statisticsId;
	private final EntryVisibilityHandler<T> visibilityHandler;

	public DataStatisticsBuilder(
			final Index index,
			final DataTypeAdapter<T> adapter,
			final DataStoreStatisticsProvider<T> statisticsProvider,
			final StatisticsId statisticsId ) {
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
		final ByteArray visibility = new ByteArray(
				visibilityHandler.getVisibility(
						entry,
						kvs));
		InternalDataStatistics<T, R, B> statistics = statisticsMap.get(visibility);
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

	public Collection<InternalDataStatistics<T, R, B>> getStatistics() {
		return statisticsMap.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kv ) {
		final ByteArray visibilityByteArray = new ByteArray(
				visibilityHandler.getVisibility(
						entry,
						kv));
		InternalDataStatistics<T, R, B> statistics = statisticsMap.get(visibilityByteArray);
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
		final ByteArray visibility = new ByteArray(
				visibilityHandler.getVisibility(
						entry,
						kv));
		InternalDataStatistics<T, R, B> statistics = statisticsMap.get(visibility);
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
