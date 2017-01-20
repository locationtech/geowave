package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

/**
 * No additional options for memory.
 */
public class MemoryRequiredOptions extends
		StoreFactoryOptions
{

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new MemoryStoreFactoryFamily();
	}
}
