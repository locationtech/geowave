package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.List;

public interface AccumuloUnreadData
{
	public List<AccumuloFieldInfo> finishRead();
}
