package mil.nga.giat.geowave.datastore.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

abstract public class AbstractCassandraStoreFactory<T> extends
		AbstractCassandraFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractCassandraStoreFactory.class);

	protected CassandraOperations createOperations(
			final CassandraRequiredOptions options ) {
		// try {
		// return CassandraOperations.createOperations(options);
		// }
		// catch (final Exception e) {
		// LOGGER.error(
		// "Unable to create Cassandra operations from config options",
		// e);
		// return null;
		// }
		return null;
	}
}
