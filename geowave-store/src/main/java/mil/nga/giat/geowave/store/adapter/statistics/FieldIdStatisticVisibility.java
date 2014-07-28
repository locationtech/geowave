package mil.nga.giat.geowave.store.adapter.statistics;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.IngestEntryInfo;
import mil.nga.giat.geowave.store.IngestEntryInfo.FieldInfo;

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
			final IngestEntryInfo entryInfo,
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
