package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;

public class MemoryDataStoreFactory extends
		AbstractMemoryStoreFactory<DataStore> implements
		DataStoreFactorySpi
{
	private static final Map<String, DataStore> DATA_STORE_CACHE = new HashMap<String, DataStore>();

	@Override
	public DataStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return createStore(namespace);
	}

	protected static synchronized DataStore createStore(
			final String namespace ) {
		DataStore store = DATA_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemoryDataStore(
					MemoryAdapterStoreFactory.createStore(namespace),
					MemoryIndexStoreFactory.createStore(namespace),
					MemoryDataStatisticsStoreFactory.createStore(namespace),
					MemorySecondaryIndexStoreFactory.createStore(namespace));
			DATA_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
