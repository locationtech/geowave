package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;

public class AccumuloDataStatisticsStoreFactory extends
		AbstractAccumuloStoreFactory<DataStatisticsStore> implements
		DataStatisticsStoreFactorySpi
{

	@Override
	public DataStatisticsStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new AccumuloDataStatisticsStore(
				createOperations(
						configOptions,
						namespace));
	}

}
