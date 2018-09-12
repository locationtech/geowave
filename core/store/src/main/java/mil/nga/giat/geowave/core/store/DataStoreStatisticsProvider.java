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
package mil.nga.giat.geowave.core.store;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.adapter.statistics.EmptyStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.PartitionStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatisticsSet;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.FieldVisibilityCount;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DataStoreStatisticsProvider<T> implements
		StatisticsProvider<T>
{
	final InternalDataAdapter<T> adapter;
	final boolean includeAdapterStats;
	final PrimaryIndex index;

	public DataStoreStatisticsProvider(
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex index,
			final boolean includeAdapterStats ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.includeAdapterStats = includeAdapterStats;
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsTypes() {
		final ByteArrayId[] idsFromAdapter;
		if ((adapter.getAdapter() instanceof StatisticsProvider) && includeAdapterStats) {
			adapter.init(index);
			idsFromAdapter = ((StatisticsProvider) adapter.getAdapter()).getSupportedStatisticsTypes();
		}
		else {
			idsFromAdapter = new ByteArrayId[0];
		}

		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 6);
		newSet[idsFromAdapter.length] = RowRangeHistogramStatistics.STATS_TYPE;
		newSet[idsFromAdapter.length + 1] = IndexMetaDataSet.STATS_TYPE;
		newSet[idsFromAdapter.length + 2] = DifferingFieldVisibilityEntryCount.STATS_TYPE;
		newSet[idsFromAdapter.length + 3] = FieldVisibilityCount.STATS_TYPE;
		newSet[idsFromAdapter.length + 4] = DuplicateEntryCount.STATS_TYPE;
		newSet[idsFromAdapter.length + 5] = PartitionStatistics.STATS_TYPE;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			final ByteArrayId statisticsType ) {
		if (statisticsType.equals(RowRangeHistogramStatistics.STATS_TYPE)) {
			return new RowRangeHistogramStatisticsSet(
					adapter.getInternalAdapterId(),
					index.getId());
		}
		if (statisticsType.equals(PartitionStatistics.STATS_TYPE)) {
			return new PartitionStatistics(
					adapter.getInternalAdapterId(),
					index.getId());
		}
		if (statisticsType.equals(IndexMetaDataSet.STATS_TYPE)) {
			return new IndexMetaDataSet(
					adapter.getInternalAdapterId(),
					index.getId(),
					index.getIndexStrategy());
		}
		if (statisticsType.equals(DifferingFieldVisibilityEntryCount.STATS_TYPE)) {
			return new DifferingFieldVisibilityEntryCount<>(
					adapter.getInternalAdapterId(),
					index.getId());
		}
		if (statisticsType.equals(FieldVisibilityCount.STATS_TYPE)) {
			return new FieldVisibilityCount<>(
					adapter.getInternalAdapterId(),
					index.getId());
		}
		if (statisticsType.equals(DuplicateEntryCount.STATS_TYPE)) {
			return new DuplicateEntryCount<>(
					adapter.getInternalAdapterId(),
					index.getId());
		}
		if (adapter.getAdapter() instanceof StatisticsProvider) {
			DataStatistics<T> stats = ((StatisticsProvider) adapter.getAdapter()).createDataStatistics(statisticsType);
			if (stats != null) {
				stats.setInternalDataAdapterId(adapter.getInternalAdapterId());
				return stats;
			}
		}
		return null;

	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			final CommonIndexModel indexModel,
			final DataAdapter<T> adapter,
			final ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter).getVisibilityHandler(
				index.getIndexModel(),
				adapter,
				statisticsId) : new EmptyStatisticVisibility<T>();
	}
}
