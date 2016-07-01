package mil.nga.giat.geowave.core.store.spi;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public interface StoreFactoryFamilySpi extends
		GenericFactory
{
	public GenericStoreFactory<DataStore> getDataStoreFactory();

	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory();

	public GenericStoreFactory<IndexStore> getIndexStoreFactory();

	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory();

	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory();

	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore();
}
