package mil.nga.giat.geowave.core.store;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface DataStoreOperations
{

	public boolean tableExists(
			final String altIdxTableName )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public String getTableNameSpace();

	public boolean mergeData(
			PrimaryIndex index,
			AdapterStore adapterStore,
			AdapterIndexMappingStore adapterIndexMappingStore );

}
