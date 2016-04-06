package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStoreFactorySpi;

public class MemoryAdapterIndexMappingStoreFactory extends
		AbstractMemoryStoreFactory<AdapterIndexMappingStore> implements
		AdapterIndexMappingStoreFactorySpi
{
	private static final Map<String, AdapterIndexMappingStore> ADAPTER_STORE_CACHE = new HashMap<String, AdapterIndexMappingStore>();

	@Override
	public AdapterIndexMappingStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return createStore(namespace);
	}

	protected static AdapterIndexMappingStore createStore(
			final String namespace ) {
		AdapterIndexMappingStore store = ADAPTER_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemoryAdapterIndexMappingStore();
			ADAPTER_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
