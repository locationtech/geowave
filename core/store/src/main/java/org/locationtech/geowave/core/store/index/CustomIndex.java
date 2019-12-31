package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.persist.Persistable;

public interface CustomIndex<E, C extends Persistable> {
  InsertionIds getInsertionIds(E entry);

  QueryRanges getQueryRanges(C constraints);
}
