package mil.nga.giat.geowave.datastore.hbase;

import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;

public class HBaseDataStoreFactory extends
		AbstractHBaseStoreFactory<DataStore> implements
		DataStoreFactorySpi
{

	@Override
	public DataStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseDataStore(
				GeoWaveStoreFinder.createIndexStore(
						configOptions,
						namespace),
				GeoWaveStoreFinder.createAdapterStore(
						configOptions,
						namespace),
				GeoWaveStoreFinder.createDataStatisticsStore(
						configOptions,
						namespace),
				GeoWaveStoreFinder.createSecondaryIndexDataStore(
						configOptions,
						namespace),
				createOperations(
						configOptions,
						namespace));
	}
}
