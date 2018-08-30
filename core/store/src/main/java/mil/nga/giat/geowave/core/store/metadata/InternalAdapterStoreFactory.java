package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;

public class InternalAdapterStoreFactory extends
		BaseStoreFactory<InternalAdapterStore>
{

	public InternalAdapterStoreFactory(
			String typeName,
			String description,
			StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public InternalAdapterStore createStore(
			final StoreFactoryOptions options ) {
		return new InternalAdapterStoreImpl(
				helper.createOperations(options));
	}

}
