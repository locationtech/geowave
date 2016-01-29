package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;

public class MemoryDataStatisticsStoreFactory extends
		AbstractMemoryStoreFactory<DataStatisticsStore> implements
		DataStatisticsStoreFactorySpi
{
	private static final Map<String, DataStatisticsStore> STATISTICS_STORE_CACHE = new HashMap<String, DataStatisticsStore>();

	@Override
	public DataStatisticsStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return createStore(namespace);
	}

	protected static DataStatisticsStore createStore(
			final String namespace ) {
		DataStatisticsStore store = STATISTICS_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemoryDataStatisticsStore();
			STATISTICS_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
