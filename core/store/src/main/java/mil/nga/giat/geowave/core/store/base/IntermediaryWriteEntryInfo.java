package mil.nga.giat.geowave.core.store.base;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;

/**
 * There is a single intermediate row per original entry passed into a write
 * operation. This offers a higher level abstraction from the raw key-value
 * pairs in geowave (can be multiple per original entry). A datastore is
 * responsible for translating from this intermediary representation of rows to
 * key-value rows.
 *
 */
class IntermediaryWriteEntryInfo
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

		public GeoWaveValue getValue() {
			return new GeoWaveValueImpl(
					dataValue.getId().getBytes(),
					visibility,
					writtenValue);
		}
	}

	private final byte[] dataId;
	private final short internalAdapterId;
	private final InsertionIds insertionIds;
	private final List<FieldInfo<?>> fieldInfo;

	public IntermediaryWriteEntryInfo(
			final byte[] dataId,
			final short internalAdapterId,
			final InsertionIds insertionIds,
			final List<FieldInfo<?>> fieldInfo ) {
		this.dataId = dataId;
		this.internalAdapterId = internalAdapterId;
		this.insertionIds = insertionIds;
		this.fieldInfo = fieldInfo;
	}

	@Override
	public String toString() {
		return new ByteArrayId(
				dataId).getString();
	}

	public short getInternalAdapterId() {
		return internalAdapterId;
	}

	public InsertionIds getInsertionIds() {
		return insertionIds;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public List<FieldInfo<?>> getFieldInfo() {
		return fieldInfo;
	}

	public GeoWaveRow[] getRows() {
		final GeoWaveValue[] fieldValues = new GeoWaveValue[fieldInfo.size()];
		for (int i = 0; i < fieldValues.length; i++) {
			fieldValues[i] = fieldInfo.get(
					i).getValue();
		}
		final GeoWaveKey[] keys = GeoWaveKeyImpl.createKeys(
				insertionIds,
				dataId,
				internalAdapterId);
		return Arrays
				.stream(
						keys)
				.map(
						k -> new GeoWaveRowImpl(
								k,
								fieldValues))
				.toArray(
						new ArrayGenerator());
	}

	private static class ArrayGenerator implements
			IntFunction<GeoWaveRow[]>
	{
		@Override
		public GeoWaveRow[] apply(
				final int value ) {
			return new GeoWaveRow[value];
		}
	}

}
