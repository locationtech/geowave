package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloDataStoreFactory extends
		DataStoreFactory
{
	public AccumuloDataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof AccumuloRequiredOptions)) {
			throw new AssertionError(
					"Expected " + AccumuloRequiredOptions.class.getSimpleName());
		}
		final AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
		if (opts.getStoreOptions() == null) {
			opts.setStoreOptions(new AccumuloOptions());
		}

		final DataStoreOperations accumuloOperations = helper.createOperations(opts);
		return new AccumuloDataStore(
				(AccumuloOperations) accumuloOperations,
				(AccumuloOptions) opts.getStoreOptions());

	}
}
