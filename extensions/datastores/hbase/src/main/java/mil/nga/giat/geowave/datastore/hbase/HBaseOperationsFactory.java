package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;

public class HBaseOperationsFactory extends
		AbstractHBaseStoreFactory<DataStoreOperations>
{

	@Override
	public DataStoreOperations createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new HBaseOptions());
		}

		return createOperations(opts);
	}
}
