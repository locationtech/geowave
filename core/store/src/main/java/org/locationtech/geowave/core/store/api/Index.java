package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * An index represents how to efficiently store and retrieve data. The common
 * index model allows for easily searching certain fields across all types
 * within an index. The numeric index strategy maps real-world values to
 * insertion keys and query ranges for efficient range scans within a key-value
 * store.
 */
public interface Index extends
		Persistable
{

	/**
	 * get the name of the index
	 *
	 * @return the name of the index (should be unique per data store)
	 */
	String getName();

	/**
	 * get the index strategy which maps real-world values to insertion keys and
	 * query ranges for efficient range scans within a key-value store.
	 *
	 * @return the numeric index strategy
	 */
	NumericIndexStrategy getIndexStrategy();

	/**
	 * The common index model allows for easily searching certain fields across
	 * all types within an index. For example, if geometry is a common index
	 * field, one could ubiquitously search all types within this index
	 * spatially. This could apply to any field type desired.
	 *
	 * @return the common index model
	 */
	CommonIndexModel getIndexModel();
}
