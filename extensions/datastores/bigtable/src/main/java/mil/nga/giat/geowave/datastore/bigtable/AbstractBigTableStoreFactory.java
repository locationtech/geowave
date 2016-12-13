package mil.nga.giat.geowave.datastore.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;

abstract public class AbstractBigTableStoreFactory<T> extends
		AbstractBigTableFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractBigTableStoreFactory.class);

	protected BigTableOperations createOperations(
			final BigTableOptions options ) {
		try {
			return BigTableOperations.createOperations(options);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create BigTable operations from config options",
					e);
			return null;
		}
	}
}
