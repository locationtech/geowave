package mil.nga.giat.geowave.datastore.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;

abstract public class AbstractDynamoDBStoreFactory<T> extends
		AbstractDynamoDBFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractDynamoDBStoreFactory.class);

	protected DynamoDBOperations createOperations(
			final DynamoDBOptions options ) {
		return new DynamoDBOperations(
				options);

	}
}
