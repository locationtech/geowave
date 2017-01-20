package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class MemoryIndexStoreFactory extends
		AbstractMemoryStoreFactory<IndexStore>
{
	private static final Map<String, IndexStore> INDEX_STORE_CACHE = new HashMap<String, IndexStore>();

	@Override
	public IndexStore createStore(
			StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
	}

	protected static synchronized IndexStore createStore(
			final String namespace ) {
		IndexStore store = INDEX_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemoryIndexStore();
			INDEX_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}