package mil.nga.giat.geowave.core.store.operations;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class DataStoreOperationsFactory extends
		BaseStoreFactory<DataStoreOperations>
{

	public DataStoreOperationsFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStoreOperations createStore(
			final StoreFactoryOptions options ) {
		return helper.createOperations(options);
	}

}
