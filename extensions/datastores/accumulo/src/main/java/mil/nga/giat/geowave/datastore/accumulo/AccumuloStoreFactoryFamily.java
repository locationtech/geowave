package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStoreFactory;

public class AccumuloStoreFactoryFamily extends
		AbstractAccumuloFactory implements
		StoreFactoryFamilySpi
{
	@Override
	public DataStoreFactorySpi getDataStoreFactory() {
		return new AccumuloDataStoreFactory();
	}

	@Override
	public DataStatisticsStoreFactorySpi getDataStatisticsStoreFactory() {
		return new AccumuloDataStatisticsStoreFactory();
	}

	@Override
	public IndexStoreFactorySpi getIndexStoreFactory() {
		return new AccumuloIndexStoreFactory();
	}

	@Override
	public AdapterStoreFactorySpi getAdapterStoreFactory() {
		return new AccumuloAdapterStoreFactory();
	}

	@Override
	public SecondaryIndexDataStoreFactorySpi getSecondaryIndexDataStore() {
		return new AccumuloSecondaryIndexDataStoreFactory();
	}

}
