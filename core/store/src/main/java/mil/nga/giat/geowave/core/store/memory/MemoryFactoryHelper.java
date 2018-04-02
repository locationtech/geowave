package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;

public class MemoryFactoryHelper implements
		StoreFactoryHelper
{
	// this operations cache is essential to re-using the same objects in memory
	private static final Map<String, DataStoreOperations> OPERATIONS_CACHE = new HashMap<String, DataStoreOperations>();

	/**
	 * Return the default options instance. This is actually a method that
	 * should be implemented by the individual factories, but is placed here
	 * since it's the same.
	 *
	 * @return
	 */
	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new MemoryRequiredOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		synchronized (OPERATIONS_CACHE) {
			DataStoreOperations operations = OPERATIONS_CACHE.get(options.getGeowaveNamespace());
			if (operations == null) {
				operations = new MemoryDataStoreOperations();
				OPERATIONS_CACHE.put(
						options.getGeowaveNamespace(),
						operations);
			}
			return operations;
		}
	}
}
