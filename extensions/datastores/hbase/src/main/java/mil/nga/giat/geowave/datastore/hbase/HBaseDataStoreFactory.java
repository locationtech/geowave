package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;

public class HBaseDataStoreFactory extends
		AbstractHBaseStoreFactory<DataStore>
{
	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		if (opts.getAdditionalOptions() == null) {
			opts.setAdditionalOptions(new HBaseOptions());
		}

		final HBaseOperations hbaseOperations = createOperations(opts);
		return new HBaseDataStore(
				new HBaseIndexStore(
						hbaseOperations),
				new HBaseAdapterStore(
						hbaseOperations),
				new HBaseDataStatisticsStore(
						hbaseOperations),
				new HBaseAdapterIndexMappingStore(
						hbaseOperations),
				new HBaseSecondaryIndexDataStore(
						hbaseOperations),
				hbaseOperations,
				opts.getAdditionalOptions());

	}
}
