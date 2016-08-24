package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemorySecondaryIndexDataStore implements
		SecondaryIndexDataStore
{

	@Override
	public void storeJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexRowId,
			ByteArrayId attributeVisibility ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			ByteArrayId attributeVisibility,
			List<FieldInfo<?>> attributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(
			SecondaryIndex<?> secondaryIndex,
			List<FieldInfo<?>> indexedAttributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeAll() {
		// TODO Auto-generated method stub

	}

	@Override
	public CloseableIterator<Object> scan(
			final ByteArrayId secondaryIndexId,
			final List<ByteArrayRange> scanRanges,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final String... visibility ) {
		// TODO Auto-generated method stub
		return null;
	}

}