package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;

public class MemoryStoreFactoryFamily extends
		AbstractMemoryFactory implements
		StoreFactoryFamilySpi
{

	@Override
	public DataStoreFactorySpi getDataStoreFactory() {
		return new MemoryDataStoreFactory();
	}

	@Override
	public DataStatisticsStoreFactorySpi getDataStatisticsStoreFactory() {
		return new MemoryDataStatisticsStoreFactory();
	}

	@Override
	public IndexStoreFactorySpi getIndexStoreFactory() {
		return new MemoryIndexStoreFactory();
	}

	@Override
	public AdapterStoreFactorySpi getAdapterStoreFactory() {
		return new MemoryAdapterStoreFactory();
	}

	@Override
	public SecondaryIndexDataStoreFactorySpi getSecondaryIndexDataStore() {
		return new MemorySecondaryIndexStoreFactory();
	}

}
