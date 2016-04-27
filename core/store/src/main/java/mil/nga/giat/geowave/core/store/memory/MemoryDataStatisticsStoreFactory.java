package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;

public class MemoryDataStatisticsStoreFactory extends
		AbstractMemoryStoreFactory<DataStatisticsStore>
{
	private static final Map<String, DataStatisticsStore> STATISTICS_STORE_CACHE = new HashMap<String, DataStatisticsStore>();

	@Override
	public DataStatisticsStore createStore(
			StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
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
