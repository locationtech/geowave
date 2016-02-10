package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;

public class HBaseAdapterStoreFactory extends
		AbstractHBaseStoreFactory<AdapterStore> implements
		AdapterStoreFactorySpi
{

	@Override
	public AdapterStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseAdapterStore(
				createOperations(
						configOptions,
						namespace));
	}

}