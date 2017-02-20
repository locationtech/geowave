package mil.nga.giat.geowave.datastore.hbase.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;

public class HBaseAdapterStoreFactory extends
		AbstractHBaseStoreFactory<AdapterStore>
{

	@Override
	public AdapterStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		return new HBaseAdapterStore(
				createOperations(opts));
	}

}