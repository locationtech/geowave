package mil.nga.giat.geowave.datastore.bigtable.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.bigtable.AbstractBigTableStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;

public class BigTableIndexStoreFactory extends
		AbstractBigTableStoreFactory<IndexStore>
{

	@Override
	public IndexStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}
		final BigTableOptions opts = (BigTableOptions) options;
		return new HBaseIndexStore(
				createOperations(opts));
	}

}
