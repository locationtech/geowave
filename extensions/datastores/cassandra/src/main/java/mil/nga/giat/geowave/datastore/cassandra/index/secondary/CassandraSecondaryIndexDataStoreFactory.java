package mil.nga.giat.geowave.datastore.cassandra.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.cassandra.AbstractCassandraStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraSecondaryIndexDataStoreFactory extends
		AbstractCassandraStoreFactory<SecondaryIndexDataStore>
{
	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}

		final CassandraOperations cassandraOperations = createOperations(
				(CassandraRequiredOptions) options);
		//TODO secondary index
		return null;
	}
}
