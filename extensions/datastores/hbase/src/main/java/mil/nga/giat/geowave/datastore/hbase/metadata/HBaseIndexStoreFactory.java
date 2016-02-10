package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.datastore.hbase.AbstractHBaseStoreFactory;

public class HBaseIndexStoreFactory extends
		AbstractHBaseStoreFactory<IndexStore> implements
		IndexStoreFactorySpi
{

	@Override
	public IndexStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new HBaseIndexStore(
				createOperations(
						configOptions,
						namespace));
	}

}
