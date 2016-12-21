package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraDataStoreFactory extends
		AbstractCassandraStoreFactory<DataStore>
{
	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}

		final CassandraOperations cassandraOperations = createOperations((CassandraRequiredOptions) options);
		// return new CassandraDataStore(
		// cassandraOperations,
		// ((CassandraOptions) options).getHBaseOptions());
		return null;

	}
}
