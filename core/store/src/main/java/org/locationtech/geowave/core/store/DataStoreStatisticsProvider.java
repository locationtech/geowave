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
package org.locationtech.geowave.core.store;

import java.util.Arrays;

import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.EmptyStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatisticsSet;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;

public class DataStoreStatisticsProvider<T> implements
		StatisticsProvider<T>
{
	final InternalDataAdapter<T> adapter;
	final boolean includeAdapterStats;
	final Index index;

	public DataStoreStatisticsProvider(
			final InternalDataAdapter<T> adapter,
			final Index index,
			final boolean includeAdapterStats ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.includeAdapterStats = includeAdapterStats;
	}

	@Override
	public StatisticsId[] getSupportedStatistics() {
		final StatisticsId[] idsFromAdapter;
		if ((adapter.getAdapter() instanceof StatisticsProvider) && includeAdapterStats) {
			idsFromAdapter = ((StatisticsProvider) adapter.getAdapter()).getSupportedStatistics();
		}
		else {
			idsFromAdapter = new StatisticsId[0];
		}

		final StatisticsId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 6);
		newSet[idsFromAdapter.length] = RowRangeHistogramStatisticsSet.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		newSet[idsFromAdapter.length + 1] = IndexMetaDataSet.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		newSet[idsFromAdapter.length + 2] = DifferingFieldVisibilityEntryCount.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		newSet[idsFromAdapter.length + 3] = FieldVisibilityCount.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		newSet[idsFromAdapter.length + 4] = DuplicateEntryCount.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		newSet[idsFromAdapter.length + 5] = PartitionStatistics.STATS_TYPE.newBuilder().indexName(
				index.getName()).build().getId();
		return newSet;
	}

	@Override
	public <R, B extends StatisticsQueryBuilder<R, B>> InternalDataStatistics<T, R, B> createDataStatistics(
			final StatisticsId statisticsId ) {
		final StatisticsType<?, ?> statisticsType = statisticsId.getType();
		if (statisticsType.equals(RowRangeHistogramStatistics.STATS_TYPE)) {
			return new RowRangeHistogramStatisticsSet(
					adapter.getAdapterId(),
					index.getName());
		}
		if (statisticsType.equals(PartitionStatistics.STATS_TYPE)) {
			return new PartitionStatistics(
					adapter.getAdapterId(),
					index.getName());
		}
		if (statisticsType.equals(IndexMetaDataSet.STATS_TYPE)) {
			return new IndexMetaDataSet(
					adapter.getAdapterId(),
					index.getName(),
					index.getIndexStrategy());
		}
		if (statisticsType.equals(DifferingFieldVisibilityEntryCount.STATS_TYPE)) {
			return new DifferingFieldVisibilityEntryCount(
					adapter.getAdapterId(),
					index.getName());
		}
		if (statisticsType.equals(FieldVisibilityCount.STATS_TYPE)) {
			return new FieldVisibilityCount(
					adapter.getAdapterId(),
					index.getName());
		}
		if (statisticsType.equals(DuplicateEntryCount.STATS_TYPE)) {
			return new DuplicateEntryCount(
					adapter.getAdapterId(),
					index.getName());
		}
		if (adapter.getAdapter() instanceof StatisticsProvider) {
			final InternalDataStatistics<T, R, B> stats = ((StatisticsProvider) adapter.getAdapter())
					.createDataStatistics(statisticsId);
			if (stats != null) {
				stats.setAdapterId(adapter.getAdapterId());
				return stats;
			}
		}
		return null;

	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			final CommonIndexModel indexModel,
			final DataTypeAdapter<T> adapter,
			final StatisticsId statisticsId ) {
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter).getVisibilityHandler(
				index.getIndexModel(),
				adapter,
				statisticsId) : new EmptyStatisticVisibility<>();
	}
}
