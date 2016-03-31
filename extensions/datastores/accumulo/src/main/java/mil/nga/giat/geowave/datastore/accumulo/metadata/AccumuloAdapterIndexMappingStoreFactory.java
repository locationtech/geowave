package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;

public class AccumuloAdapterIndexMappingStoreFactory extends
		AbstractAccumuloStoreFactory<AdapterIndexMappingStore> implements
		AdapterIndexMappingStoreFactorySpi
{

	@Override
	public AdapterIndexMappingStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new AccumuloAdapterIndexMappingStore(
				createOperations(
						configOptions,
						namespace));
	}

}