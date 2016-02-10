package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.util.Map;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.HBaseOptions;

public class HBaseSecondaryIndexDataStoreFactory extends
		AbstractHBaseStoreFactory<SecondaryIndexDataStore> implements
		SecondaryIndexDataStoreFactorySpi
{

	@Override
	public SecondaryIndexDataStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseSecondaryIndexDataStore(
				createOperations(
						configOptions,
						namespace),
				new HBaseOptions());
	}
}
