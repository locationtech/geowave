package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;

public class MemoryAdapterStoreFactory extends
		AbstractMemoryStoreFactory<AdapterStore>
{
	private static final Map<String, AdapterStore> ADAPTER_STORE_CACHE = new HashMap<String, AdapterStore>();

	@Override
	public AdapterStore createStore(
			StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
	}

	protected static AdapterStore createStore(
			final String namespace ) {
		AdapterStore store = ADAPTER_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemoryAdapterStore();
			ADAPTER_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
