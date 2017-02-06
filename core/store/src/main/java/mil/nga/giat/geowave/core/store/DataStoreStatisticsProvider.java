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
	public ByteArrayId[] getSupportedStatisticsTypes() {
		final ByteArrayId[] idsFromAdapter;
		if ((adapter instanceof StatisticsProvider) && includeAdapterStats) {
			idsFromAdapter = ((StatisticsProvider) adapter).getSupportedStatisticsTypes();
		}
		else {
			idsFromAdapter = new ByteArrayId[0];
		}

		final ByteArrayId[] newSet = Arrays.copyOf(
				idsFromAdapter,
				idsFromAdapter.length + 5);
		newSet[idsFromAdapter.length] = RowRangeDataStatistics.STATS_TYPE;
		newSet[idsFromAdapter.length + 1] = RowRangeHistogramStatistics.STATS_TYPE;
		newSet[idsFromAdapter.length + 2] = IndexMetaDataSet.STATS_TYPE;
		newSet[idsFromAdapter.length + 3] = DifferingFieldVisibilityEntryCount.STATS_TYPE;
		newSet[idsFromAdapter.length + 4] = DuplicateEntryCount.STATS_TYPE;
		return newSet;
	}

	@Override
	public DataStatistics<T> createDataStatistics(
			final ByteArrayId statisticsType ) {
		if (statisticsType.equals(RowRangeDataStatistics.STATS_TYPE)) {
			return new RowRangeDataStatistics(
					index.getId());
		}
		if (statisticsType.equals(RowRangeHistogramStatistics.STATS_TYPE)) {
			return new RowRangeHistogramStatistics(
					adapter.getAdapterId(),
					index.getId(),
					1024);
		}
		if (statisticsType.equals(IndexMetaDataSet.STATS_TYPE)) {
			return new IndexMetaDataSet(
					adapter.getAdapterId(),
					index.getId(),
					index.getIndexStrategy());
		}
		if (statisticsType.equals(DifferingFieldVisibilityEntryCount.STATS_TYPE)) {
			return new DifferingFieldVisibilityEntryCount<>(
					adapter.getAdapterId(),
					index.getId());
		}
		if (statisticsType.equals(DuplicateEntryCount.STATS_TYPE)) {
			return new DuplicateEntryCount<>(
					adapter.getAdapterId(),
					index.getId());
		}
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter)
				.createDataStatistics(statisticsType) : null;
	}

	@Override
	public EntryVisibilityHandler<T> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return (adapter instanceof StatisticsProvider) ? ((StatisticsProvider) adapter)
				.getVisibilityHandler(statisticsId) : new EmptyStatisticVisibility<T>();
	}
}