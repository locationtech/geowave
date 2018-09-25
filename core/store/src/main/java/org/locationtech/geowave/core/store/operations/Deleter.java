package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public interface Deleter<T> extends
		Reader<T>,
		ScanCallback<T, GeoWaveRow>
{
}
