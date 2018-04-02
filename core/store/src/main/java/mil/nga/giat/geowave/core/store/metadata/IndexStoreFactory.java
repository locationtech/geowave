package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class IndexStoreFactory extends
		BaseStoreFactory<IndexStore>
{

	public IndexStoreFactory(
			String typeName,
			String description,
			StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public IndexStore createStore(
			final StoreFactoryOptions options ) {
		return new IndexStoreImpl(
				helper.createOperations(options),
				options.getStoreOptions());
	}

}
