package mil.nga.giat.geowave.core.store.operations;

import java.util.Iterator;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface Reader<T> extends
		AutoCloseable,
		Iterator<T>
{

}
