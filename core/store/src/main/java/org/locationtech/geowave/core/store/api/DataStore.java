/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.api;

import java.net.URL;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;

/**
 * A DataStore can both ingest and query data based on persisted indices and
 * data adapters. When the data is ingested it is explicitly given an index and
 * a data adapter which is then persisted to be used in subsequent queries.
 * 
 */
public interface DataStore
{	
	/**
	 * Returns an index writer to perform batched write operations
	 * 
	 * @param dataTypeAdapter
	 *            The adapter that describes the data written to the set of
	 *            indices.
	 * @param index
	 *            The configuration information for the primary index to use.
	 * @return Returns the index writer which can be used for batch write
	 *         operations
	 */
	public <T> void ingest(
			URL url,
			@Nullable IngestOptions<T> options,
			Index... index )
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
	 * @param query
	 *            data constraints for the query and additional options for the
	 *            processing the query
	 * @return An iterator on all results that match the query. The iterator
	 *         implements Closeable and it is best practice to close the
	 *         iterator after it is no longer needed.
	 */
	public <T> CloseableIterator<T> query(
			final Query<T> query );

	

	//TODO javadocs
	public <P extends Persistable, R extends Mergeable, T> R aggregate(
			final AggregationQuery<P,R,T> query );
	
/**
	 * Delete all data in this data store that matches the query parameter
	 * within the index described by the index passed in and matches the adapter
	 * (the same adapter ID as the ID ingested). All data that matches the
	 * query, adapter ID, and is in the index ID will be deleted.
	 * 
	 * For ({@link org.locationtech.geowave.core.store.query.AdapterIdQuery),
	 * all supporting statistics and secondary indices are also deleted.
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
	public <T> boolean delete(
			final Query<T> query );

	/**
	 * Get all the adapters that have been used within this data store
	 * 
	 * @return An array of the adapters used within this datastore.
	 */
	DataTypeAdapter<?>[] getTypes();

	/**
	 * Get all data statistics for a given adapter ID using the authorizations
	 * provided. If adapter ID is null, it will return all statistics in this
	 * data store.
	 * 
	 * @param dataTypeId
	 *            The Adapter to get the accumulated statistics for all the data
	 *            that has been ingested. If null, it will return all data
	 *            statistics.
	 * @param authorizations
	 *            A set of authorizations to use for access to the statistics
	 * @return An iterator on the data statistics. The iterator implements
	 *         Closeable and it is best practice to close the iterator after it
	 *         is no longer needed.
	 */
	Statistics<?>[] getStatistics(
			@Nullable String typeName,
			String... authorizations );

	/**
	 * Get all data statistics for a given adapter ID using the authorizations
	 * provided. If adapter ID is null, it will return all statistics in this
	 * data store.
	 * 
	 * @param dataTypeAdapterName
	 *            The Adapter to get the accumulated statistics for all the data
	 *            that has been ingested. If null, it will return all data
	 *            statistics.
	 * @param statisticsId
	 *            the data statistics ID
	 * @param authorizations
	 *            A set of authorizations to use for access to the atatistics
	 * @return An iterator on the data statistics. The iterator implements
	 *         Closeable and it is best practice to close the iterator after it
	 *         is no longer needed.
	 */
	<R> R getStatisticsResult(
			String typeName,
			StatisticsType<R> statisticsType,
			String... authorizations );

	/**
	 * Get the indices that have been used within this data store for a
	 * particular adapter ID. Note that once data has been written for an
	 * adapter with a set of indices, that set of indices must be consistently
	 * used for subsequent writes (otherwise a user will run the risk of
	 * inconsistent data). If data type ID is null it will return all
	 * statistics.
	 * 
	 * @param dataTypeId
	 *            an data type adapter ID to find the relevant indices, or all
	 *            indices if data type ID is null
	 * 
	 * @return An array of the indices for a given data type.
	 */
	Index[] getIndices(
			@Nullable String typeName );
	
	void copy(Query<?> query, DataStore other);
	
	void addIndex(String typeName, Index ...indices);
	
	<T> void addType(DataTypeAdapter<T> dataTypeAdapter);
	/**
	 * Returns an index writer to perform batched write operations for the given typename
	 * 
	 */
	public <T> Writer<T> createWriter(String typeName);
}
