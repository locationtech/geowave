package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.persist.Persistable;

public interface DataTypeQueryOptions<T> extends
		Persistable
{

	public String[] getTypeNames();

}
