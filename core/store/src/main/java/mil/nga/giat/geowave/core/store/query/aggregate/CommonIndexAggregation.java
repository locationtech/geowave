package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public interface CommonIndexAggregation<P extends Persistable, R extends Mergeable> extends
		Aggregation<P, R, CommonIndexedPersistenceEncoding>
{
}
