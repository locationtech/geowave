package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.util.Map;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOptions;

public class AccumuloSecondaryIndexDataStoreFactory extends
		AbstractAccumuloStoreFactory<SecondaryIndexDataStore> implements
		SecondaryIndexDataStoreFactorySpi
{

	@Override
	public SecondaryIndexDataStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new AccumuloSecondaryIndexDataStore(
				createOperations(
						configOptions,
						namespace),
				new AccumuloOptions());
	}
}
