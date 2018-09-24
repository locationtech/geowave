package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public interface Index extends
		Persistable
{

	String getId();

	NumericIndexStrategy getIndexStrategy();

	CommonIndexModel getIndexModel();
}
