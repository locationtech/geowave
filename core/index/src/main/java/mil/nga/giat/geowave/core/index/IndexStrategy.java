package mil.nga.giat.geowave.core.index;

import java.util.List;

public interface IndexStrategy extends
		Persistable
{
	public List<IndexMetaData> createMetaData();

	/**
	 *
	 * @return a unique ID associated with the index strategy
	 */
	public String getId();
}
