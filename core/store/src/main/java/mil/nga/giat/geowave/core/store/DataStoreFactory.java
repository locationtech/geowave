package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;

public class DataStoreFactory extends
		BaseStoreFactory<DataStore>
{
	public DataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStore createStore(
			final StoreFactoryOptions factoryOptions ) {
		final DataStoreOperations operations = helper.createOperations(factoryOptions);
		final DataStoreOptions options = factoryOptions.getStoreOptions();
		return new BaseDataStore(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new SecondaryIndexStoreImpl(),
				operations,
				options,
				new InternalAdapterStoreImpl(
						operations));
	}
}
