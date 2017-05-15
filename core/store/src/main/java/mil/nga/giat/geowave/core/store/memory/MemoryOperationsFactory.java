package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class MemoryOperationsFactory extends
		AbstractMemoryStoreFactory<DataStoreOperations>
{
	private static final Map<String, DataStoreOperations> OPERATIONS_CACHE = new HashMap<String, DataStoreOperations>();

	@Override
	public DataStoreOperations createStore(
			final StoreFactoryOptions configOptions ) {
		return createStore(
				configOptions.getGeowaveNamespace());
	}

	protected static DataStoreOperations createStore(
			final String namespace ) {
		DataStoreOperations operations = OPERATIONS_CACHE.get(
				namespace);
		if (operations == null) {
			operations = new MemoryStoreOperations();
			OPERATIONS_CACHE.put(
					namespace,
					operations);
		}
		return operations;
	}
}
