package mil.nga.giat.geowave.datastore.accumulo.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class AccumuloAdapterStoreFactory extends
		AbstractAccumuloStoreFactory<AdapterStore>
{

	@Override
	public AdapterStore createStore(
			StoreFactoryOptions options ) {
		if (!(options instanceof AccumuloRequiredOptions)) {
			throw new AssertionError(
					"Expected " + AccumuloRequiredOptions.class.getSimpleName());
		}
		AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
		return new AccumuloAdapterStore(
				createOperations(opts));
	}

}