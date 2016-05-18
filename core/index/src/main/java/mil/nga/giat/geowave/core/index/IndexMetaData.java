package mil.nga.giat.geowave.core.index;

import java.util.List;

public interface IndexMetaData extends
		Persistable,
		Mergeable
{
	/**
	 * Update the aggregation result using the new entry provided
	 * 
	 * @param entry
	 *            the new entry to compute an updated aggregation result on
	 */
	public void update(
			List<ByteArrayId> ids );
}
