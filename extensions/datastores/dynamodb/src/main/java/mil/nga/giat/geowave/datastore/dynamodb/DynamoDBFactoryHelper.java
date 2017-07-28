package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;

public class DynamoDBFactoryHelper implements
		StoreFactoryHelper
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DynamoDBFactoryHelper.class);

	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new DynamoDBOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		try {
			return DynamoDBOperations.createOperations((DynamoDBOptions) options);
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to create DynamoDB operations from config options",
					e);
			return null;
		}
	}

}
