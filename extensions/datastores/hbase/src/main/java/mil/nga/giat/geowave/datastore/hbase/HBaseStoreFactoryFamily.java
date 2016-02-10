package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStoreFactory;

public class HBaseStoreFactoryFamily extends
		AbstractHBaseFactory implements
		StoreFactoryFamilySpi
{
	@Override
	public DataStoreFactorySpi getDataStoreFactory() {
		return new HBaseDataStoreFactory();
	}

	@Override
	public DataStatisticsStoreFactorySpi getDataStatisticsStoreFactory() {
		return new HBaseDataStatisticsStoreFactory();
	}

	@Override
	public IndexStoreFactorySpi getIndexStoreFactory() {
		return new HBaseIndexStoreFactory();
	}

	@Override
	public AdapterStoreFactorySpi getAdapterStoreFactory() {
		return new HBaseAdapterStoreFactory();
	}

	@Override
	public SecondaryIndexDataStoreFactorySpi getSecondaryIndexDataStore() {
		return new HBaseSecondaryIndexDataStoreFactory();
	}

}
