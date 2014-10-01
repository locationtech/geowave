package mil.nga.giat.geowave.store;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.PersistentValue;

public class IngestEntryInfo
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

	private final List<ByteArrayId> rowIds;
	private final List<FieldInfo> fieldInfo;

	public IngestEntryInfo(
			final List<ByteArrayId> rowIds,
			final List<FieldInfo> fieldInfo ) {
		this.rowIds = rowIds;
		this.fieldInfo = fieldInfo;
	}

	public List<ByteArrayId> getRowIds() {
		return rowIds;
	}

	public List<FieldInfo> getFieldInfo() {
		return fieldInfo;
	}
}
