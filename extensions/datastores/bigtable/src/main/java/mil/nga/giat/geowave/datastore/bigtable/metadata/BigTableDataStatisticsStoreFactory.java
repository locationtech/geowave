package mil.nga.giat.geowave.datastore.bigtable.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.bigtable.AbstractBigTableStoreFactory;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;

public class BigTableDataStatisticsStoreFactory extends
		AbstractBigTableStoreFactory<DataStatisticsStore>
{

	@Override
	public DataStatisticsStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}
		final BigTableOptions opts = (BigTableOptions) options;
		return new HBaseDataStatisticsStore(
				createOperations(opts));
	}
}
