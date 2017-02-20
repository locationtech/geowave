package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;

/**
 * This assigns the visibility of the key-value with the most-significant field
 * bitmask (the first fields in the bitmask are the indexed fields, and all
 * indexed fields should be the default visibility which should be the minimal
 * set of visibility contraints of any field)
 *
 * @param <T>
 *            The field type
 */
public class DefaultFieldStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{
	private static List<GeoWaveValue> getAllVisibilities(
			final GeoWaveRow... kvs ) {
		List<GeoWaveValue> retVal = new ArrayList<>();
		for (GeoWaveRow kv : kvs) {
			for (GeoWaveValue v : kv.getFieldValues()) {
				retVal.add(v);
			}
		}
		return retVal;
	}

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveRow... kvs ) {
		List<GeoWaveValue> allVis = getAllVisibilities(kvs);
		if (allVis.size() == 1) {
			return allVis.get(
					0).getVisibility();
		}
		int lowestOrdinal = Integer.MAX_VALUE;
		byte[] lowestOrdinalVisibility = null;
		for (GeoWaveValue v : allVis) {
			final int pos = BitmaskUtils.getLowestFieldPosition(v.getFieldMask());
			if (pos == 0) {
				return v.getVisibility();
			}
			if (pos <= lowestOrdinal) {
				lowestOrdinal = pos;
				lowestOrdinalVisibility = v.getVisibility();
			}
		}
		return lowestOrdinalVisibility;
	}
}
