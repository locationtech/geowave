package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;

public class HBaseSecondaryIndexDataStoreFactory extends
		AbstractHBaseStoreFactory<SecondaryIndexDataStore>
{

	@Override
	public SecondaryIndexDataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new HBaseOptions());
		}
		return new HBaseSecondaryIndexDataStore(
				createOperations(opts));
	}
}
