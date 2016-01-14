package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

public class FieldTypeStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{
	private final Class<?> fieldType;

	public FieldTypeStatisticVisibility(
			final Class<?> fieldType ) {
		this.fieldType = fieldType;
	}

	@Override
	public byte[] getVisibility(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final FieldInfo<?> f : entryInfo.getFieldInfo()) {
			if ((f.getDataValue().getValue() != null) && fieldType.isAssignableFrom(f.getDataValue().getValue().getClass())) {
				return f.getVisibility();
			}
		}
		return null;
	}

}
