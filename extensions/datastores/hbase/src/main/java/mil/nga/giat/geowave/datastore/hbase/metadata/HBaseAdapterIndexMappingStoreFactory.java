package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;

public class HBaseAdapterIndexMappingStoreFactory extends
		AbstractHBaseStoreFactory<AdapterIndexMappingStore> implements
		AdapterIndexMappingStoreFactorySpi
{

	@Override
	public AdapterIndexMappingStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseAdapterIndexMappingStore(
				createOperations(
						configOptions,
						namespace));
	}

}