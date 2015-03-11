package mil.nga.giat.geowave.store.adapter.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.DeleteCallback;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.ScanCallback;

public class DataStatisticsBuilder<T> implements
		IngestCallback<T>,
		DeleteCallback<T>,
		ScanCallback<T>
{
	private final StatisticalDataAdapter<T> adapter;
	private final Map<ByteArrayId, DataStatistics<T>> statisticsMap = new HashMap<ByteArrayId, DataStatistics<T>>();
	private final ByteArrayId statisticsId;
	private final DataStatisticsVisibilityHandler<T> visibilityHandler;

	public DataStatisticsBuilder(
			final StatisticalDataAdapter<T> adapter,
			final ByteArrayId statisticsId ) {
		this.adapter = adapter;
		this.statisticsId = statisticsId;
		this.visibilityHandler = adapter.getVisibilityHandler(statisticsId);
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = adapter.createDataStatistics(statisticsId);
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entryInfo,
				entry);
	}

	public Collection<DataStatistics<T>> getStatistics() {
		return statisticsMap.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final ByteArrayId visibilityByteArray = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibilityByteArray);
		if (statistics == null) {
			statistics = adapter.createDataStatistics(statisticsId);
			statistics.setVisibility(visibilityByteArray.getBytes());
			statisticsMap.put(
					visibilityByteArray,
					statistics);
		}
		if (statistics instanceof DeleteCallback) {
			((DeleteCallback<T>) statistics).entryDeleted(
					entryInfo,
					entry);
		}
	}

	@Override
	public void entryScanned(
			DataStoreEntryInfo entryInfo,
			T entry ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = adapter.createDataStatistics(statisticsId);
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entryInfo,
				entry);
		
	}

}
