package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreFactory;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloSecondaryIndexDataStoreFactory extends
		SecondaryIndexStoreFactory
{
	public AccumuloSecondaryIndexDataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public SecondaryIndexDataStore createStore(
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
		return new AccumuloSecondaryIndexDataStore(
				(AccumuloOperations) accumuloOperations,
				(AccumuloOptions) opts.getStoreOptions());
	}
}
