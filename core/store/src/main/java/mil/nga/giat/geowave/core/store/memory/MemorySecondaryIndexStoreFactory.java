package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemorySecondaryIndexStoreFactory extends
		AbstractMemoryStoreFactory<SecondaryIndexDataStore>
{
	private static final Map<String, SecondaryIndexDataStore> STATISTICS_STORE_CACHE = new HashMap<String, SecondaryIndexDataStore>();

	@Override
	public SecondaryIndexDataStore createStore(
			StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
	}

	protected static SecondaryIndexDataStore createStore(
			final String namespace ) {
		SecondaryIndexDataStore store = STATISTICS_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemorySecondaryIndexDataStore();
			STATISTICS_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
