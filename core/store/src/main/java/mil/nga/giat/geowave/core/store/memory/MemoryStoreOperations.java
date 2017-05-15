package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This is just a facade to get tests passing, it is not meant for actual usage
 *
 *
 */
public class MemoryStoreOperations implements
		DataStoreOperations
{

	@Override
	public boolean tableExists(
			final String altIdxTableName )
			throws IOException {
		return false;
	}

	@Override
	public void deleteAll()
			throws Exception {
	}

	@Override
	public String getTableNameSpace() {
		return null;
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return true;
	}

}
