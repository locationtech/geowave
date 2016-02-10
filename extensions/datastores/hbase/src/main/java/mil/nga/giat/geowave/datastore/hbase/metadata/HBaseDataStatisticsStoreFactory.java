package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;

public class HBaseDataStatisticsStoreFactory extends
		AbstractHBaseStoreFactory<DataStatisticsStore> implements
		DataStatisticsStoreFactorySpi
{

	@Override
	public DataStatisticsStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseDataStatisticsStore(
				createOperations(
						configOptions,
						namespace));
	}

}
