package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStoreStatsAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataAdapter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataManager;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class DataStoreCallbackManager implements
		AutoCloseable
{

	public static final int FLUSH_STATS_THRESHOLD = 16384;

	final private DataStatisticsStore statsStore;
	private boolean persistStats = true;
	final private SecondaryIndexDataStore secondaryIndexStore;

	final Map<ByteArrayId, IngestCallback<?>> icache = new HashMap<ByteArrayId, IngestCallback<?>>();
	final Map<ByteArrayId, DeleteCallback<?>> dcache = new HashMap<ByteArrayId, DeleteCallback<?>>();

	public DataStoreCallbackManager(
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexStore ) {
		this.statsStore = statsStore;
		this.secondaryIndexStore = secondaryIndexStore;
	}

	public <T> IngestCallback<T> getIngestCallback(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!icache.containsKey(writableAdapter.getAdapterId())) {
			final DataStoreStatsAdapterWrapper<T> statsAdapter = new DataStoreStatsAdapterWrapper<T>(
					index,
					writableAdapter);
			final List<IngestCallback<T>> callbackList = new ArrayList<IngestCallback<T>>();
			if ((writableAdapter instanceof StatisticalDataAdapter) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsAdapter,
						statsStore));
			}
			if (writableAdapter instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter,
						index.getId()));
			}
			icache.put(
					writableAdapter.getAdapterId(),
					new IngestCallbackList<T>(
							callbackList));
		}
		return (IngestCallback<T>) icache.get(writableAdapter.getAdapterId());

	}

	public void setPersistStats(
			final boolean persistStats ) {
		this.persistStats = persistStats;
	}

	public <T> DeleteCallback<T> getDeleteCallback(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!dcache.containsKey(writableAdapter.getAdapterId())) {
			final DataStoreStatsAdapterWrapper<T> statsAdapter = new DataStoreStatsAdapterWrapper<T>(
					index,
					writableAdapter);
			final List<DeleteCallback<T>> callbackList = new ArrayList<DeleteCallback<T>>();
			if ((writableAdapter instanceof StatisticalDataAdapter) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsAdapter,
						statsStore));
			}
			if (writableAdapter instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter,
						index.getId()));
			}
			dcache.put(
					writableAdapter.getAdapterId(),
					new DeleteCallbackList<T>(
							callbackList));
		}
		return (DeleteCallback<T>) dcache.get(writableAdapter.getAdapterId());

	}

	@Override
	public void close()
			throws IOException {
		for (final IngestCallback<?> callback : icache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
		for (final DeleteCallback<?> callback : dcache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}
}
