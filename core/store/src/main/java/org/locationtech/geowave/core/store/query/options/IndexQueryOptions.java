package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.persist.Persistable;

public interface IndexQueryOptions extends
		Persistable
{
	public String getIndexName();

	public boolean isAllIndicies();
}
