package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.dynamodb.AbstractDynamoDBStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBAdapterStoreFactory extends
		AbstractDynamoDBStoreFactory<AdapterStore>
{

	@Override
	public AdapterStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;
		return new DynamoDBAdapterStore(
				createOperations(opts));
	}

}