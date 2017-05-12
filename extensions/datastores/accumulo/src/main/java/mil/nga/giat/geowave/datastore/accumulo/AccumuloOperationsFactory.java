package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

public class AccumuloOperationsFactory extends
		AbstractAccumuloStoreFactory<DataStoreOperations>
{

	@Override
	public DataStoreOperations createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof AccumuloRequiredOptions)) {
			throw new AssertionError(
					"Expected " + AccumuloRequiredOptions.class.getSimpleName());
		}
		final AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new AccumuloOptions());
		}

		return createOperations(opts);

	}
}
