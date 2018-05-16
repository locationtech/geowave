package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;

public class DynamoDBDataStoreFactory extends
		DataStoreFactory
{
	public DynamoDBDataStoreFactory(
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
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;

		final DataStoreOperations dynamodbOperations = helper.createOperations(opts);

		return new DynamoDBDataStore(
				(DynamoDBOperations) dynamodbOperations);

	}
}
