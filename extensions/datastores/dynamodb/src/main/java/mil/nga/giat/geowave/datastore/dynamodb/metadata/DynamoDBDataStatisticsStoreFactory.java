package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.dynamodb.AbstractDynamoDBStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBDataStatisticsStoreFactory extends
		AbstractDynamoDBStoreFactory<DataStatisticsStore>
{

	@Override
	public DataStatisticsStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof DynamoDBOptions)) {
			throw new AssertionError(
					"Expected " + DynamoDBOptions.class.getSimpleName());
		}
		final DynamoDBOptions opts = (DynamoDBOptions) options;
		return new DynamoDBDataStatisticsStore(
				createOperations(opts));
	}

}
