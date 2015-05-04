package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.index.Index;

/**
 * This is a visitor that can interpret the compatibility of an index
 * 
 */
public interface IndexCompatibilityVisitor
{
	/**
	 * Determine whether an index is compatible with the visitor
	 * 
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether this index is compatible with the visitor
	 */
	public boolean isCompatible(
			Index index );
}
