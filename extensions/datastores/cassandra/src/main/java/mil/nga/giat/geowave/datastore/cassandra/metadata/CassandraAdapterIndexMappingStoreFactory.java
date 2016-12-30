package mil.nga.giat.geowave.datastore.cassandra.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.cassandra.AbstractCassandraStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraAdapterIndexMappingStoreFactory extends
		AbstractCassandraStoreFactory<AdapterIndexMappingStore>
{
	@Override
	public AdapterIndexMappingStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}

		final CassandraOperations cassandraOperations = createOperations((CassandraRequiredOptions) options);
		return new CassandraAdapterIndexMappingStore(
				cassandraOperations);
	}
}