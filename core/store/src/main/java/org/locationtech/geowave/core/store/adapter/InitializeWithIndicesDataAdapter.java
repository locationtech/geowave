package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;

public interface InitializeWithIndicesDataAdapter<T> extends
		DataTypeAdapter<T>
{

	boolean init(
			Index... indices );
}
