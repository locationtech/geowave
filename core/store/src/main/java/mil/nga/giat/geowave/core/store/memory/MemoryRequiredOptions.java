package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

/**
 * No additional options for memory.
 */
public class MemoryRequiredOptions extends
		StoreFactoryOptions
{
	private final DataStoreOptions options = new BaseDataStoreOptions();

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new MemoryStoreFactoryFamily();
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return options;
	}
}
