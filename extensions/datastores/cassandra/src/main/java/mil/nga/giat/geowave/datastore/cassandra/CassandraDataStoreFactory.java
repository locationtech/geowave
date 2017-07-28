package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraDataStoreFactory extends
		DataStoreFactory
{

	public CassandraDataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}

		return new CassandraDataStore(
				(CassandraOperations) helper.createOperations(options),
				(CassandraOptions) options.getStoreOptions());
	}
}
