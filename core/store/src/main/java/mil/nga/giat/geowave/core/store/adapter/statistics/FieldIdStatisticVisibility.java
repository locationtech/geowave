package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

public class FieldIdStatisticVisibility<T> implements
		DataStatisticsVisibilityHandler<T>
{
	private final ByteArrayId fieldId;

	public FieldIdStatisticVisibility(
			final ByteArrayId fieldId ) {
		this.fieldId = fieldId;
	}

	@Override
	public byte[] getVisibility(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final FieldInfo<T> f : entryInfo.getFieldInfo()) {
			if (f.getDataValue().getId().equals(
					fieldId)) {
				return f.getVisibility();
			}
		}
		return null;
	}
}
