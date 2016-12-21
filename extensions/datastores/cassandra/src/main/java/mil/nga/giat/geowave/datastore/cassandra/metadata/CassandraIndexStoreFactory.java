package mil.nga.giat.geowave.datastore.cassandra.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.cassandra.AbstractCassandraStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraIndexStoreFactory extends
		AbstractCassandraStoreFactory<IndexStore>
{

	@Override
	public IndexStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}
		final CassandraOperations cassandraOperations = createOperations(
				(CassandraRequiredOptions) options);
		return new CassandraIndexStore(
				cassandraOperations);
	}

}
