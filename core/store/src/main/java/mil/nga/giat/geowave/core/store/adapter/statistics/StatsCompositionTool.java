package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DeleteCallback;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

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
		ScanCallback<T>,
		DeleteCallback<T>,
		AutoCloseable
{
	DataStatisticsStore statisticsStore;
	List<DataStatisticsBuilder<T>> statisticsBuilders = null;
	final boolean persistStats;
	final Object MUTEX = new Object();

	public StatsCompositionTool() {
		statisticsStore = null;
		persistStats = false;
	}

	public StatsCompositionTool(
			final DataAdapter<T> dataAdapter ) {
		this.persistStats = true;
		this.statisticsStore = null;
		this.init(dataAdapter);
	}

	public StatsCompositionTool(
			final DataAdapter<T> dataAdapter,
			final DataStatisticsStore statisticsStore ) {
		this.statisticsStore = statisticsStore;
		persistStats = (dataAdapter instanceof StatisticalDataAdapter) && (statisticsStore != null);
		this.init(dataAdapter);
	}

	private void init(
			final DataAdapter<T> dataAdapter ) {
		if (persistStats) {
			final ByteArrayId[] statisticsIds = ((StatisticalDataAdapter<T>) dataAdapter).getSupportedStatisticsIds();
			statisticsBuilders = new ArrayList<DataStatisticsBuilder<T>>(
					statisticsIds.length);
			for (final ByteArrayId id : statisticsIds) {
				statisticsBuilders.add(new DataStatisticsBuilder<T>(
						(StatisticalDataAdapter<T>) dataAdapter,
						id));
			}
		}
	}

	public boolean isPersisting() {
		return persistStats;
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (statisticsBuilders == null) {
			return;
		}
		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryDeleted(
						entryInfo,
						entry);
			}
		}

	}

	@Override
	public void entryScanned(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryScanned(
						entryInfo,
						entry);
			}
		}

	}

	/**
	 * Update statistics store
	 */
	public void flush() {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				final Collection<DataStatistics<T>> statistics = builder.getStatistics();
				for (final DataStatistics<T> s : statistics) {
					statisticsStore.incorporateStatistics(s);
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
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (statisticsBuilders == null) {
			return;
		}

		synchronized (MUTEX) {
			for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
				builder.entryIngested(
						entryInfo,
						entry);
			}
		}
	}

	@Override
	public void close()
			throws Exception {
		flush();
	}

	public void setStatisticsStore(
			DataStatisticsStore statisticsStore ) {
		this.statisticsStore = statisticsStore;
	}

}
