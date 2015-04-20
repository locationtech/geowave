package mil.nga.giat.geowave.core.store.query;

import mil.nga.giat.geowave.core.index.Persistable;

/**
 * This interface fully describes a query and is persistable so that it can be
 * distributed if necessary (particularly useful for using a query as mapreduce
 * input)
 */
public interface DistributableQuery extends
		Query,
		Persistable
{

}
