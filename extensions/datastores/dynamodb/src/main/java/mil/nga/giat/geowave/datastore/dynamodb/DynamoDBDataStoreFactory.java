package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBDataStatisticsStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBIndexStore;

public class DynamoDBDataStoreFactory extends
		AbstractDynamoDBStoreFactory<DataStore>
{

	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;

		final DynamoDBOperations dynamodbOperations = createOperations(opts);
		return new DynamoDBDataStore(
				dynamodbOperations);

	}
}
