package org.locationtech.geowave.core.geotime.util;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

public class IndexOptimizationUtils
{

	public static boolean hasAtLeastSpatial(
			final Index index ) {
		if ((index == null) || (index.getIndexModel() == null) || (index.getIndexModel().getDimensions() == null)) {
			return false;
		}
		boolean hasLatitude = false;
		boolean hasLongitude = false;
		for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
			if (dimension instanceof LatitudeField) {
				hasLatitude = true;
			}
			if (dimension instanceof LongitudeField) {
				hasLongitude = true;
			}
		}
		return hasLatitude && hasLongitude;
	}

	public static boolean hasTime(
			final Index index,
			final GeotoolsFeatureDataAdapter adapter ) {
		return hasTime(index) && adapter.hasTemporalConstraints();
	}

	public static boolean hasTime(
			final Index index ) {
		if ((index == null) || (index.getIndexModel() == null) || (index.getIndexModel().getDimensions() == null)) {
			return false;
		}
		for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
			if (dimension instanceof TimeField) {
				return true;
			}
		}
		return false;
	}
}
