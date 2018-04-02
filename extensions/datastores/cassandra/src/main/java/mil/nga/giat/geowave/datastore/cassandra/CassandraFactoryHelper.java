package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraFactoryHelper implements
		StoreFactoryHelper
{
	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new CassandraRequiredOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		return new CassandraOperations(
				(CassandraRequiredOptions) options);
	}

}