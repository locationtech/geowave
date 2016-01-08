package mil.nga.giat.geowave.datastore.accumulo.index.secondary;

import java.util.Map;

import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;

public class AccumuloSecondaryIndexDataStoreFactory extends
		AbstractAccumuloStoreFactory<SecondaryIndexDataStore> implements
		SecondaryIndexDataStoreFactorySpi
{

	@Override
	public SecondaryIndexDataStore createStore(
			Map<String, Object> configOptions,
			String namespace ) {
		return new AccumuloSecondaryIndexDataStore(
				createOperations(
						configOptions,
						namespace));
	}
}
