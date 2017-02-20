package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.SortedIndexStrategy;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.QueryConstraints;

public interface Index<QueryRangeType extends QueryConstraints, EntryRangeType> extends
		Persistable
{
	public ByteArrayId getId();

	public SortedIndexStrategy<QueryRangeType, EntryRangeType> getIndexStrategy();

}
