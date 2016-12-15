package mil.nga.giat.geowave.datastore.bigtable;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;

public class BigTableDataStoreFactory extends
		AbstractBigTableStoreFactory<DataStore>
{
	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}

		final BigTableOperations bigTableOperations = createOperations((BigTableOptions) options);
		return new HBaseDataStore(
				bigTableOperations,
				((BigTableOptions) options).getHBaseOptions());

	}
}
