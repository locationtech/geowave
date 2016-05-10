package mil.nga.giat.geowave.core.store;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public interface DataStoreOperations
{

	public boolean tableExists(
			final String altIdxTableName )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public String getTableNameSpace();

}
