package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.Map;

import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.datastore.accumulo.AbstractAccumuloStoreFactory;

public class AccumuloIndexStoreFactory extends
		AbstractAccumuloStoreFactory<IndexStore> implements
		IndexStoreFactorySpi
{

	@Override
	public IndexStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		return new AccumuloIndexStore(
				createOperations(
						configOptions,
						namespace));
	}

}
