/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.statistics.StatisticType;

/**
 * A DataStore can both ingest and query data based on persisted indices and data type adapters.
 * When the data is ingested it is explicitly given an index and a data type adapter which is then
 * persisted to be used in subsequent queries. Also, implicitly statistics are maintained associated
 * with all data ingested. These statistics can be queried. Furthermore, aggregations can be applied
 * directly to the data which are similar to statistics, but are more dynamic in that any query
 * criteria can be applied as the input of the aggregation. Data stores that support server-side
 * processing will run the aggregation within the scope of iterating through the results for
 * additional efficiency.
 *
 * <p>Here is a simple snippet of pseudocode showing how a data store can be used to store and
 * retrieve your data.
 *
 * <pre>
 * {@code
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
 * }
 * </pre>
 *
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
   * Perform a query using the GeoWave Query Language (GWQL).
   * 
   * @param queryStr the GWQL query to perform
   * @param authorizations the authorizations to use for the query
   * @return the set of results that match the given query string
   */
  ResultSet query(final String queryStr, final String... authorizations);

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
   * Add a statistic to the data store. The initial value of the statistic will not be calculated
   * and if there is existing relevant data, this statistic will not be accurate without forcing a
   * calculation. If instead it is not desire-able to calculate on add use {@code addStatistic}
   * instead.
   *
   * @param statistics the statistics to add
   */
  void addEmptyStatistic(Statistic<?>... statistic);

  /**
   * Add a statistic to the data store. The initial value of the statistic will be calculated after
   * being added. If this calculation is not desired use {@code addEmptyStatistic} instead.
   *
   * @param statistics the statistics to add
   */
  void addStatistic(Statistic<?>... statistic);

  /**
   * Remove statistics from the data store.
   *
   * @param statistic the statistics to remove
   */
  void removeStatistic(final Statistic<?>... statistic);

  /**
   * Force a recomputation of the stats
   *
   * @param statistic the statistics to recompute
   */
  void recalcStatistic(Statistic<?>... statistic);

  /**
   * Gets all of the statistics that are being tracked on the provided data type adapter.
   *
   * @param typeName the data type adapter to get the statistics for
   * @return An array of all the statistics that are being tracked on the provided data type
   *         adapter. Note this is the descriptors of the statistics, not the values.
   */
  DataTypeStatistic<?>[] getDataTypeStatistics(final String typeName);

  /**
   * Gets the statistic that is being tracked for the data type, statistic type, and tag specified.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param statisticType the statistic type for the statistic to get
   * @param typeName the data type name to get the statistic for
   * @param tag the tag of the statistic, if not specified, a tag will be inferred
   * @return the statistic, or null if no statistic matches the criteria
   */
  <V extends StatisticValue<R>, R> DataTypeStatistic<V> getDataTypeStatistic(
      final StatisticType<V> statisticType,
      final String typeName,
      @Nullable final String tag);

  /**
   * Gets all of the statistics that are being tracked on the provided index.
   *
   * @param indexName the index name to retrieve statistics for
   * @return An array of all the statistics that are being tracked on the provided index. Note this
   *         is the descriptors of the statistics, not the values.
   */
  IndexStatistic<?>[] getIndexStatistics(final String indexName);

  /**
   * Gets the statistic that is being tracked for the index, statistic type, and tag specified.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param statisticType the statistic type for the statistic to get
   * @param indexName
   * @param tag the tag of the statistic, if not specified, a tag will be inferred
   * @return the statistic, or null if no statistic matches the criteria
   */
  <V extends StatisticValue<R>, R> IndexStatistic<V> getIndexStatistic(
      final StatisticType<V> statisticType,
      final String indexName,
      @Nullable final String tag);

  /**
   * Gets all of the statistics that are being tracked on the provided type/field pair.
   *
   * @param typeName the data type name to get the statistics for
   * @param fieldName the field name to get the statistics for
   * @return An array of all the statistics that are being tracked on the provided field. Note this
   *         is the descriptors of the statistics, not the values.
   */
  FieldStatistic<?>[] getFieldStatistics(final String typeName, final String fieldName);

  /**
   * Gets the statistic that is being tracked for the data type, field, statistic type, and tag
   * specified.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param statisticType the statistic type for the statistic to get
   * @param typeName the data type name to get the statistic for
   * @param fieldName
   * @param tag the tag of the statistic, if not specified, a tag will be inferred
   * @return the statistic, or null if no statistic matches the criteria
   */
  <V extends StatisticValue<R>, R> FieldStatistic<V> getFieldStatistic(
      final StatisticType<V> statisticType,
      final String typeName,
      final String fieldName,
      @Nullable final String tag);

  /**
   * The statistic value of this stat (if multiple bins match, it will automatically aggregate the
   * resulting values together). For statistics with bins, it will always aggregate all bins.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param stat the statistic to get the value for
   * @return the statistic's value, aggregated together if there are multiple matching values.
   */
  default <V extends StatisticValue<R>, R> R getStatisticValue(final Statistic<V> stat) {
    return getStatisticValue(stat, BinConstraints.allBins());
  }

  /**
   * The statistic value of this stat (if multiple bins match, it will automatically aggregate the
   * resulting values together).
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param stat the statistic to get the value for
   * @param binConstraints the bin(s) to get the value for based on the constraints
   * @return the statistic's value, aggregated together if there are multiple matching values.
   */
  <V extends StatisticValue<R>, R> R getStatisticValue(
      Statistic<V> stat,
      BinConstraints binConstraints);

  /**
   * Returns all of the statistic values of this stat as well as the associated bin. It will return
   * each individual match as a bin-value pair.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param stat the statistic to get the value for
   * @return the statistic bin-value pairs, if there are multiple matching values which should only
   *         be the case for different bins it will return each individual value. It will return an
   *         empty iterator if there are no matching values.
   */
  default <V extends StatisticValue<R>, R> CloseableIterator<Pair<ByteArray, R>> getBinnedStatisticValues(
      final Statistic<V> stat) {
    return getBinnedStatisticValues(stat, BinConstraints.allBins());
  }

  /**
   * The statistic values of this stat as well as the associated bin. If multiple bins match, it
   * will return each individual match as a bin-value pair.
   *
   * @param <V> the StatisticValue implementation of the statistic
   * @param <R> the raw value type of the statistic
   * @param stat the statistic to get the value for
   * @param binConstraints the bin(s) to get the value for based on the constraints
   * @return the statistic bin-value pairs, if there are multiple matching values which should only
   *         be the case for different bins it will return each individual value. It will return an
   *         empty iterator if there are no matching values.
   */
  <V extends StatisticValue<R>, R> CloseableIterator<Pair<ByteArray, R>> getBinnedStatisticValues(
      Statistic<V> stat,
      BinConstraints binConstraints);

  /**
   * Get data statistics that match the given query criteria
   *
   * @param query the query criteria, use StatisticQueryBuilder or its extensions and if you're
   *        interested in a particular common statistics type use StatisticsQueryBuilder.factory()
   * @return An array of statistics that result from the query
   */
  <V extends StatisticValue<R>, R> CloseableIterator<V> queryStatistics(StatisticQuery<V, R> query);

  /**
   * Get a single statistical result that matches the given query criteria
   *
   * @param query the query criteria, use StatisticQueryBuilder or its extensions and if you're
   *        interested in a particular common statistics type use StatisticsQueryBuilder.factory()
   * @return If the query does not define that statistics type it will return null as aggregation
   *         only makes sense within a single type, otherwise aggregates the results of the query
   *         into a single result that is returned
   */
  <V extends StatisticValue<R>, R> V aggregateStatistics(StatisticQuery<V, R> query);

  /**
   * Add an index to the data store.
   *
   * @param index the index to add
   */
  void addIndex(Index index);

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
   * @param the data type name
   *
   * @return An array of the indices for a given data type.
   */
  Index[] getIndices(String typeName);

  /**
   * Get a particular index by its index name. If one doesn't exist it will return null.
   *
   * @param indexName the index name for which to retrieve an index
   * @return The index matching the specified index name or null if it doesn't exist
   */
  Index getIndex(String indexName);

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
   * Add this type to the data store with the given statistics. This only needs to be called one
   * time ever per type.
   *
   * @param dataTypeAdapter the data type adapter for this type that is used to read and write
   *        GeoWave entries
   * @param statistics the initial set of statistics that will be used with this adapter
   * @param initialIndices the initial indexing for this type, in the future additional indices can
   *        be added
   */
  <T> void addType(
      DataTypeAdapter<T> dataTypeAdapter,
      List<Statistic<?>> statistics,
      Index... initialIndices);

  /**
   * Add this type to the data store with the given statistics and visibility handler. This only
   * needs to be called one time ever per type.
   *
   * @param dataTypeAdapter the data type adapter for this type that is used to read and write
   *        GeoWave entries
   * @param visibilityHandler the visibility handler for the adapter entries
   * @param statistics the initial set of statistics that will be used with this adapter
   * @param initialIndices the initial indexing for this type, in the future additional indices can
   *        be added
   */
  <T> void addType(
      DataTypeAdapter<T> dataTypeAdapter,
      VisibilityHandler visibilityHandler,
      List<Statistic<?>> statistics,
      Index... initialIndices);

  /**
   * Returns an index writer to perform batched write operations for the given data type name. It
   * assumes the type has already been used previously or added using addType and assumes one or
   * more indices have been provided for this type.
   *
   * @param typeName the type
   * @return a writer which can be used to write entries into this datastore of the given type
   */
  <T> Writer<T> createWriter(String typeName);

  /**
   * Returns an index writer to perform batched write operations for the given data type name. It
   * assumes the type has already been used previously or added using addType and assumes one or
   * more indices have been provided for this type.
   *
   * @param typeName the type
   * @param visibilityHandler the visibility handler for newly written entries
   * @return a writer which can be used to write entries into this datastore of the given type
   */
  <T> Writer<T> createWriter(String typeName, VisibilityHandler visibilityHandler);
}
