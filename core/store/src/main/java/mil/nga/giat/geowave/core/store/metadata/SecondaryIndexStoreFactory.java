package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class SecondaryIndexStoreFactory extends
		BaseStoreFactory<SecondaryIndexDataStore>
{
	public SecondaryIndexStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		return new SecondaryIndexStoreImpl();
	}
}
