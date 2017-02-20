package mil.nga.giat.geowave.datastore.hbase.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;

public class HBaseAdapterIndexMappingStoreFactory extends
		AbstractHBaseStoreFactory<AdapterIndexMappingStore>
{

	@Override
	public AdapterIndexMappingStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new HBaseOptions());
		}
		return new HBaseAdapterIndexMappingStore(
				createOperations(opts));
	}

}