package mil.nga.giat.geowave.datastore.accumulo.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class AccumuloAdapterIndexMappingStoreFactory extends
		AbstractAccumuloStoreFactory<AdapterIndexMappingStore>
{

	@Override
	public AdapterIndexMappingStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof AccumuloRequiredOptions)) {
			throw new AssertionError(
					"Expected " + AccumuloRequiredOptions.class.getSimpleName());
		}
		AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new AccumuloOptions());
		}
		return new AccumuloAdapterIndexMappingStore(
				createOperations(opts));
	}

}