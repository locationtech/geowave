package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;

public interface StoreFactoryFamilySpi extends
		GenericFactory
{
	public DataStoreFactorySpi getDataStoreFactory();

	public DataStatisticsStoreFactorySpi getDataStatisticsStoreFactory();

	public IndexStoreFactorySpi getIndexStoreFactory();

	public AdapterStoreFactorySpi getAdapterStoreFactory();

	public SecondaryIndexDataStoreFactorySpi getSecondaryIndexDataStore();
}
