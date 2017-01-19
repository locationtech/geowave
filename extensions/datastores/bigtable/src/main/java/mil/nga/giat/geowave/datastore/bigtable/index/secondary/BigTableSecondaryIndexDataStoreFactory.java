package mil.nga.giat.geowave.datastore.bigtable.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.bigtable.AbstractBigTableStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;

public class BigTableSecondaryIndexDataStoreFactory extends
		AbstractBigTableStoreFactory<SecondaryIndexDataStore>
{
	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}

		final BigTableOperations bigTableOperations = createOperations((BigTableOptions) options);
		return new HBaseSecondaryIndexDataStore(
				bigTableOperations,
				((BigTableOptions) options).getHBaseOptions());
	}
}
