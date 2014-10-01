package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.store.IngestEntryInfo;
import mil.nga.giat.geowave.store.IngestEntryInfo.FieldInfo;

public class FieldTypeStatisticVisibility<T> implements
		DataStatisticsVisibilityHandler<T>
{
	private final Class<?> fieldType;

	public FieldTypeStatisticVisibility(
			final Class<?> fieldType ) {
		this.fieldType = fieldType;
	}

	@Override
	public byte[] getVisibility(
			final IngestEntryInfo entryInfo,
			final T entry ) {
		for (final FieldInfo<T> f : entryInfo.getFieldInfo()) {
			if ((f.getDataValue().getValue() != null) && fieldType.isAssignableFrom(f.getDataValue().getValue().getClass())) {
				return f.getVisibility();
			}
		}
		return null;
	}

}
