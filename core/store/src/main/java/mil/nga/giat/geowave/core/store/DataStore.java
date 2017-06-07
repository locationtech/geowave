/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

/**
 * A DataStore can both ingest and query data based on persisted indices and
 * data adapters. When the data is ingested it is explicitly given an index and
 * a data adapter which is then persisted to be used in subsequent queries.
 */
public interface DataStore
{
	/**
	 * Returns an index writer to perform batched write operations
	 * 
	 * @param adapter
	 *            The adapter that describes the data written to the set of
	 *            indices.
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @return Returns the index writer which can be used for batch write
	 *         operations
	 */
	public <T> IndexWriter createWriter(
			DataAdapter<T> adapter,
			PrimaryIndex... index )
			throws MismatchedIndexToAdapterMapping;

	/**
	 * Returns all data in this data store that matches the query parameter
	 * within the index described within the QueryOptions. If by the index
	 * passed in and matches the adapter (the same adapter ID as the ID
	 * ingested). All data that matches the query, adapter ID, and is in the
	 * index ID will be returned as an instance of the native data type that
	 * this adapter supports. The iterator will only return as many results as
	 * the limit passed in.
	 * 
	 * @param queryOptions
	 *            additional options for the processing the query
	 * @param the
	 *            data constraints for the query
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query );

/**
	 * Delete all data in this data store that matches the query parameter
	 * within the index described by the index passed in and matches the adapter
	 * (the same adapter ID as the ID ingested). All data that matches the
	 * query, adapter ID, and is in the index ID will be deleted.
	 * 
	 * For ({@link mil.nga.giat.geowave.core.store.query.AdapterIdQuery), all
	 * supporting statistics and secondary indices are also deleted.
	 * 
	 * Statistics and secondary indices are updated as required for all other
	 * types of queries.
	 * 
	 * 
	 * @param queryOptions
	 *            additional options for the processing the query
	 * @param the
	 *            data constraints for the query
	 * @return true on success
	 */
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query );

}
