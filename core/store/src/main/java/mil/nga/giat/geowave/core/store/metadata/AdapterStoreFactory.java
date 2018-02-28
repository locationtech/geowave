package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;

public class AdapterStoreFactory extends
		BaseStoreFactory<AdapterStore>
{

	public AdapterStoreFactory(
			String typeName,
			String description,
			StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public AdapterStore createStore(
			final StoreFactoryOptions options ) {
		return new AdapterStoreImpl(
				helper.createOperations(options),
				options.getStoreOptions());
	}

}
