package mil.nga.giat.geowave.core.store.base;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

public class DataStoreEntryInfo
{
	public static class FieldInfo<T>
	{
		private final PersistentValue<T> dataValue;
		private final byte[] visibility;
		private final byte[] writtenValue;

		public FieldInfo(
				final PersistentValue<T> dataValue,
				final byte[] writtenValue,
				final byte[] visibility ) {
			this.dataValue = dataValue;
			this.writtenValue = writtenValue;
			this.visibility = visibility;
		}

		public PersistentValue<T> getDataValue() {
			return dataValue;
		}

		public byte[] getWrittenValue() {
			return writtenValue;
		}

		public byte[] getVisibility() {
			return visibility;
		}
	}

	private final byte[] dataId;
	private final List<ByteArrayId> rowIds;
	private final List<ByteArrayId> insertionIds;
	private final List<FieldInfo<?>> fieldInfo;

	public DataStoreEntryInfo(
			final byte[] dataId,
			final List<ByteArrayId> insertionIds,
			final List<ByteArrayId> rowIds,
			final List<FieldInfo<?>> fieldInfo ) {
		this.dataId = dataId;
		this.insertionIds = insertionIds;
		this.rowIds = rowIds;
		this.fieldInfo = fieldInfo;
	}

	public List<ByteArrayId> getInsertionIds() {
		return insertionIds;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public List<ByteArrayId> getRowIds() {
		return rowIds;
	}

	public List<FieldInfo<?>> getFieldInfo() {
		return fieldInfo;
	}
}
