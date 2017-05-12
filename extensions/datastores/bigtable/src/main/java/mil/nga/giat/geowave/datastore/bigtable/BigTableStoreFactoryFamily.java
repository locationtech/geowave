package mil.nga.giat.geowave.datastore.bigtable;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.bigtable.index.secondary.BigTableSecondaryIndexDataStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.metadata.BigTableAdapterIndexMappingStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.metadata.BigTableAdapterStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.metadata.BigTableDataStatisticsStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.metadata.BigTableIndexStoreFactory;

public class BigTableStoreFactoryFamily extends
		AbstractBigTableFactory implements
		StoreFactoryFamilySpi
{
	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new BigTableDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new BigTableDataStatisticsStoreFactory();
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new BigTableIndexStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory() {
		return new BigTableAdapterStoreFactory();
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new BigTableSecondaryIndexDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new BigTableAdapterIndexMappingStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory() {
		return new BigTableOperationsFactory();
	}
}
