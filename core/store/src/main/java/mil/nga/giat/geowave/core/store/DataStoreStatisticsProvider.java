package mil.nga.giat.geowave.core.store;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.adapter.statistics.EmptyStatisticVisibility;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.FieldVisibilityCount;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DataStoreStatisticsProvider<T> implements
		StatisticsProvider<T>
{
	final DataAdapter<T> adapter;
	final boolean includeAdapterStats;
	final PrimaryIndex index;

	public DataStoreStatisticsProvider(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final boolean includeAdapterStats ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.includeAdapterStats = includeAdapterStats;
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		final ByteArrayId[] idsFromAdapter = ((adapter instanceof StatisticsProvider) && includeAdapterStats) ? ((StatisticsProvider) adapter)
				.getSupportedStatisticsIds() : new ByteArrayId[0];
		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 5);
		newSet[idsFromAdapter.length] = RowRangeDataStatistics.STATS_ID;
		newSet[idsFromAdapter.length + 1] = RowRangeHistogramStatistics.STATS_ID;
		newSet[idsFromAdapter.length + 2] = IndexMetaDataSet.STATS_ID;
		newSet[idsFromAdapter.length + 3] = DifferingFieldVisibilityEntryCount.STATS_ID;
		newSet[idsFromAdapter.length + 4] = DuplicateEntryCount.STATS_ID;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			final ByteArrayId statisticsId ) {
		if (statisticsId.equals(RowRangeDataStatistics.STATS_ID)) {
			return new RowRangeDataStatistics(
					index.getId());
		}
		if (statisticsId.equals(RowRangeHistogramStatistics.STATS_ID)) {
			return new RowRangeHistogramStatistics(
					adapter.getAdapterId(),
					index.getId(),
					1024);
		}
		if (statisticsId.equals(IndexMetaDataSet.STATS_ID)) {
			return new IndexMetaDataSet(
					adapter.getAdapterId(),
					index.getId(),
					index.getIndexStrategy());
		}
		if (statisticsId.equals(DifferingFieldVisibilityEntryCount.STATS_ID)) {
			return new DifferingFieldVisibilityEntryCount<>(
					adapter.getAdapterId(),
					index.getId());
		}
		if (statisticsId.equals(DuplicateEntryCount.STATS_ID)) {
			return new DuplicateEntryCount<>(
					adapter.getAdapterId(),
					index.getId());
		}
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter)
				.createDataStatistics(statisticsId) : null;
	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter)
				.getVisibilityHandler(statisticsId) : new EmptyStatisticVisibility<T>();
	}
}