package mil.nga.giat.geowave.core.store.filter;

import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;

/**
 * A simple filter interface to determine inclusion/exclusion based on a generic
 * persistence encoding. Client-side filters will be given an
 * AdapterPersistenceEncoding but distributable filters will be given a generic
 * PersistenceEncoding.
 * 
 */
public interface QueryFilter
{
	public boolean accept(
			IndexedPersistenceEncoding persistenceEncoding );
}
