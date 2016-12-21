package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterIndexMappingStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBDataStatisticsStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBIndexStoreFactory;

public class DynamoDBStoreFactoryFamily extends
		AbstractDynamoDBFactory implements
		StoreFactoryFamilySpi
{
	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new DynamoDBDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new DynamoDBDataStatisticsStoreFactory();
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new DynamoDBIndexStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory() {
		return new DynamoDBAdapterStoreFactory();
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return null;
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new DynamoDBAdapterIndexMappingStoreFactory();
	}

}
