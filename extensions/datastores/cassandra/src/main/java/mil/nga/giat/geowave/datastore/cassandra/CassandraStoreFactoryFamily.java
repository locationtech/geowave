package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.cassandra.index.secondary.CassandraSecondaryIndexDataStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterIndexMappingStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraDataStatisticsStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraIndexStoreFactory;

public class CassandraStoreFactoryFamily extends
		AbstractCassandraFactory implements
		StoreFactoryFamilySpi
{
	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new CassandraDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new CassandraDataStatisticsStoreFactory();
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new CassandraIndexStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory() {
		return new CassandraAdapterStoreFactory();
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new CassandraSecondaryIndexDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new CassandraAdapterIndexMappingStoreFactory();
	}
}
