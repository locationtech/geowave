package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class FieldIdStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	private final int bitPosition;

	public FieldIdStatisticVisibility(
			final ByteArrayId fieldId,
			final CommonIndexModel model,
			final DataAdapter adapter ) {
		this.bitPosition = adapter.getPositionOfOrderedField(
				model,
				fieldId);
	}

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow r : kvs) {
			for (final GeoWaveValue v : r.getFieldValues()) {
				final List<Integer> positions = BitmaskUtils.getFieldPositions(v.getFieldMask());
				if (positions.contains(bitPosition)) {
					return v.getVisibility();
				}
			}
		}
		return null;
	}
}
