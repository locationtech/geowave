package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;

public interface StoreFactoryHelper
{
	public StoreFactoryOptions createOptionsInstance();

	public DataStoreOperations createOperations(
			StoreFactoryOptions options );
}
