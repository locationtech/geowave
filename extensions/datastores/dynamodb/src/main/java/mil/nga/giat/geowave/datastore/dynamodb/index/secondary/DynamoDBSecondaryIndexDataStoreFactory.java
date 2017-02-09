package mil.nga.giat.geowave.datastore.dynamodb.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.AbstractDynamoDBStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBSecondaryIndexDataStoreFactory extends
		AbstractDynamoDBStoreFactory<SecondaryIndexDataStore>
{
	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}

		final DynamoDBOperations dynamoDBOperations = createOperations((DynamoDBOptions) options);
		return new DynamoDBSecondaryIndexDataStore(
				dynamoDBOperations);
	}
}
