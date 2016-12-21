package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.dynamodb.AbstractDynamoDBStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBIndexStoreFactory extends
		AbstractDynamoDBStoreFactory<IndexStore>
{

	@Override
	public IndexStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;
		return new DynamoDBIndexStore(
				createOperations(opts));
	}

}
