/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;

/**
 * A DataStore can both ingest and query data based on persisted indices and data type adapters.
 * When the data is ingested it is explicitly given an index and a data type adapter which is then
 * persisted to be used in subsequent queries. Also, implicitly statistics are maintained associated
 * with all data ingested. These statistics can be queried. Furthermore, aggregations can be applied
 * directly to the data which are similar to statistics, but are more dynamic in that any query
 * criteria can be applied as the input of the aggregation. Datastores that support serverside
 * processing will run the aggregation within the scope of iterating through the results for
 * additional efficiency.
 *
 * <p>Here is a simple snippets of pseudocode showing how a data store can be used to store and
 * retrieve your data.
 *
 * @formatter:off
 *     <pre>{@code
 *  DataStore store = DataStoreFactory.createDataStore(<data store options>);
 * 	store.addType(<my data type>, <my index>);
 *  try(Writer writer = store.createWriter()){
 *    //write data
 *    writer.writer(<data);
 *  }
 *
 *  //this just queries everything
 *  try(CloseableIterator it = store.query(QueryBuilder.newBuilder().build())){
 *    while(it.hasNext()){
 *      //retrieve results matching query criteria and do something
 *    	it.next();
 *    }
 *  }
 * }</pre>
 *
 * @formatter:on
 */
public interface DataStore {

  /**
   * Ingest from path. If this is a directory, this method will recursively search for valid files
   * to ingest in the directory. This will iterate through registered IngestFormatPlugins to find
   * one that works for a given file. The applicable ingest format plugin will choose the
   * DataTypeAdapter and may even use additional indices than the one provided.
   *
   * @param inputPath The path for data to read and ingest into this data store
   * @param index The indexing approach to use.
   */
  <T> void ingest(String inputPath, Index... index);

  /**
   * Ingest from path with options. If this is a directory, this method will recursively search for
   * valid files to ingest in the directory. The applicable ingest format plugin will choose the
   * DataTypeAdapter and may even use additional indices than the one provided.
   *
   * @param inputPath The path for data to read and ingest into this data store
   * @param options a set of available options for ingesting from a URL
   * @param index The configuration information for the primary index to use.
   */
  <T> void ingest(String inputPath, IngestOptions<T> options, Index... index);

  /**
   * Returns all data in this data store that matches the query parameter. All data that matches the
   * query will be returned as an instance of the native data type. The Iterator must be closed when
   * it is no longer needed - this wraps the underlying scanner implementation and closes underlying
   * resources.
   *
   * @param query data constraints for the query and additional options for processing the query
   * @return An iterator on all results that match the query. The iterator implements Closeable and
   *         it is best practice to close the iterator after it is no longer needed.
   */
  <T> CloseableIterator<T> query(final Query<T> query);

  /**
   * Perform an aggregation on the data and just return the aggregated result. The query criteria is
   * very similar to querying the individual entries except in this case it defines the input to the
   * aggregation function, and the aggregation function produces a single result. Examples of this
   * might be simply counting matched entries, producing a bounding box or other range/extent for
   * matched entries, or producing a histogram.
   *
   * @param query the Aggregation Query, use AggregationQueryBuilder or its extensions to create
   * @return the single result of the aggregation
   */
  <P extends Persistable, R, T> R aggregate(final AggregationQuery<P, R, T> query);

  /**
   * Get the data type adapter with the given type name from the data store.
   * 
   * @param typeName the name of the type to get
   * @return The data type adapter with the given name, or {@code null} if it couldn't be found
   */
  DataTypeAdapter<?> getType(final String typeName);

  /**
   * Get all the data type adapters that have been used within this data store
   *
   * @return An array of the types used within this datastore.
   */
  DataTypeAdapter<?>[] getTypes();

  /**
   * Get data statistics that match the given query criteria
   *
   * @param query the query criteria, use StatisticsQueryBuilder or its extensions and if you're
   *        interested in a particular common statistics type use StatisticsQueryBuilder.factory()
   * @return An array of statistics that result from the query
   */
  <R> Statistics<R>[] queryStatistics(StatisticsQuery<R> query);

  /**
   * Get a single statistical result that matches the given query criteria
   *
   * @param query the query criteria, use StatisticsQueryBuilder or its extensions and if you're
   *        interested in a particular common statistics type use StatisticsQueryBuilder.factory()
   * @return If the query does not define that statistics type it will return null as aggregation
   *         only makes sense within a single type, otherwise aggregates the results of the query
   *         into a single result that is returned
   */
  <R> R aggregateStatistics(StatisticsQuery<R> query);

  /**
   * Get the indices that have been used within this data store.
   *
   * @return all indices used within this datastore
   */
  Index[] getIndices();

  /**
   * Get the indices that have been used within this data store for a particular type. If data type
   * name is null it will return all indices.
   *
   * @return An array of the indices for a given data type.
   */
  Index[] getIndices(String typeName);

  /**
   * copy all data from this store into a specified other store
   *
   * @param other the other store to copy data into
   */
  void copyTo(DataStore other);

  /**
   * copy the subset of data matching this query from this store into a specified other store
   *
   * @param other the other store to copy data into
   * @param query a query to select which data to copy - use QueryBuilder or its extension to create
   */
  void copyTo(DataStore other, Query<?> query);

  /**
   * Add new indices for the given type. If there is data in other indices for this type, for
   * consistency it will need to copy all of the data into the new indices, which could be a long
   * process for lots of data.
   *
   * @param typeName the type
   * @param indices the new indices to add
   */
  void addIndex(String typeName, Index... indices);

  /**
   * remove an index completely for all types. If this is the last index for any type it throws an
   * illegal state exception, expecting the user to remove the type before removing the index to
   * protect a user from losing any reference to their data unknowingly for a type.
   *
   * @param indexName the index
   * @throws IllegalStateException if this is the last index for a type, remove the type first
   */
  void removeIndex(String indexName) throws IllegalStateException;

  /**
   * remove an index for the given type. If this is the last index for that type it throws an
   * illegal state exception, expecting the user to remove the type before removing the index to
   * protect a user from losing any reference to their data unknowingly for a type.
   *
   * @param typeName the type
   * @param indexName the index
   * @throws IllegalStateException if this is the last index for a type, remove the type first
   */
  void removeIndex(String typeName, String indexName) throws IllegalStateException;

  /**
   * Remove all data and statistics associated with the given type.
   *
   * @param typeName the type
   */
  void removeType(String typeName);

  /**
   * Delete all data in this data store that matches the query parameter.
   *
   * <p> Statistics are updated as required.
   *
   * @param query the query criteria to use for deletion
   * @return true on success
   */
  <T> boolean delete(final Query<T> query);

  /**
   * Delete ALL data and ALL metadata for this datastore. This is provided for convenience as a
   * simple way to wipe a datastore cleanly, but don't be surprised if everything is gone.
   */
  void deleteAll();

  /**
   * Add this type to the data store. This only needs to be called one time ever per type.
   *
   * @param dataTypeAdapter the data type adapter for this type that is used to read and write
   *        GeoWave entries
   * @param initialIndices the initial indexing for this type, in the future additional indices can
   *        be added
   */
  <T> void addType(DataTypeAdapter<T> dataTypeAdapter, Index... initialIndices);

  /**
   * Returns an index writer to perform batched write operations for the given data type name. It
   * assumes the type has already been used previously or added using addType and assumes one or
   * more indices have been provided for this type.
   *
   * @param typeName the type
   * @return a writer which can be used to write entries into this datastore of the given type
   */
  <T> Writer<T> createWriter(String typeName);
}
