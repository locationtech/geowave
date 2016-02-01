package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;

public class AccumuloAdapterStoreFactory extends
		AbstractAccumuloStoreFactory<AdapterStore> implements
		AdapterStoreFactorySpi
{

	@Override
	public AdapterStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new AccumuloAdapterStore(
				createOperations(
						configOptions,
						namespace));
	}

}