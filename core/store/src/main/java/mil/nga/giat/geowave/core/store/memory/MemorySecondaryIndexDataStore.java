package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemorySecondaryIndexDataStore implements
		SecondaryIndexDataStore
{

	@Override
	public void store(
			SecondaryIndex<?> secondaryIndex,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexRowId,
			List<FieldInfo<?>> indexedAttributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(
			SecondaryIndex<?> secondaryIndex,
			List<FieldInfo<?>> indexedAttributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public CloseableIterator<ByteArrayId> query(
			SecondaryIndex<?> secondaryIndex,
			List<ByteArrayRange> ranges,
			List<DistributableQueryFilter> constraints,
			ByteArrayId primaryIndexId,
			String... visibility ) {
		return new CloseableIterator.Empty<ByteArrayId>();
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
