package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.dynamodb.AbstractDynamoDBStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBAdapterIndexMappingStoreFactory extends
		AbstractDynamoDBStoreFactory<AdapterIndexMappingStore>
{

	@Override
	public AdapterIndexMappingStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		DynamoDBOptions opts = (DynamoDBOptions) options;
		return new DynamoDBAdapterIndexMappingStore(
				createOperations(opts));
	}

}