package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;

public class AdapterIndexMappingStoreFactory extends
		BaseStoreFactory<AdapterIndexMappingStore>
{

	public AdapterIndexMappingStoreFactory(
			String typeName,
			String description,
			StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public AdapterIndexMappingStore createStore(
			final StoreFactoryOptions options ) {
		return new AdapterIndexMappingStoreImpl(
				helper.createOperations(options),
				options.getStoreOptions());
	}

}
