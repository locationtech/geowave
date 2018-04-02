package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.BaseStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;

public class DataStatisticsStoreFactory extends
		BaseStoreFactory<DataStatisticsStore>
{
	public DataStatisticsStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStatisticsStore createStore(
			final StoreFactoryOptions options ) {
		return new DataStatisticsStoreImpl(
				helper.createOperations(options),
				options.getStoreOptions());
	}
}
