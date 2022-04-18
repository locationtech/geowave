/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.PropertyStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.server.ServerOpHelper;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;
import org.locationtech.geowave.mapreduce.splits.SplitsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Accumulo implementation of the data store. It requires an AccumuloOperations instance
 * that describes how to connect (read/write data) to Apache Accumulo. It can create default
 * implementations of the IndexStore and AdapterStore based on the operations which will persist
 * configuration information to Accumulo tables, or an implementation of each of these stores can be
 * passed in A DataStore can both ingest and query data based on persisted indices and data
 * adapters. When the data is ingested it is explicitly given an index and a data adapter which is
 * then persisted to be used in subsequent queries.
 */
public class AccumuloDataStore extends BaseMapReduceDataStore implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStore.class);

  public AccumuloDataStore(
      final AccumuloOperations accumuloOperations,
      final AccumuloOptions accumuloOptions) {
    super(
        new IndexStoreImpl(accumuloOperations, accumuloOptions),
        new AdapterStoreImpl(accumuloOperations, accumuloOptions),
        new DataStatisticsStoreImpl(accumuloOperations, accumuloOptions),
        new AdapterIndexMappingStoreImpl(accumuloOperations, accumuloOptions),
        accumuloOperations,
        accumuloOptions,
        new InternalAdapterStoreImpl(accumuloOperations),
        new PropertyStoreImpl(accumuloOperations, accumuloOptions));
  }

  @Override
  protected SplitsProvider createSplitsProvider() {
    return new AccumuloSplitsProvider();
  }

  @Override
  protected void initOnIndexWriterCreate(final InternalDataAdapter adapter, final Index index) {
    final String indexName = index.getName();
    final String typeName = adapter.getTypeName();
    try {
      if (adapter.getAdapter() instanceof RowMergingDataAdapter) {
        if (!((AccumuloOperations) baseOperations).isRowMergingEnabled(
            adapter.getAdapterId(),
            indexName)) {
          if (!((AccumuloOperations) baseOperations).createTable(
              indexName,
              false,
              baseOptions.isEnableBlockCache())) {
            ((AccumuloOperations) baseOperations).enableVersioningIterator(indexName, false);
          }
          if (baseOptions.isServerSideLibraryEnabled()) {
            ServerOpHelper.addServerSideRowMerging(
                ((RowMergingDataAdapter<?, ?>) adapter.getAdapter()),
                adapter.getAdapterId(),
                (ServerSideOperations) baseOperations,
                RowMergingCombiner.class.getName(),
                RowMergingVisibilityCombiner.class.getName(),
                indexName);
          }
        }
      }
      if (((AccumuloOptions) baseOptions).isUseLocalityGroups()
          && !((AccumuloOperations) baseOperations).localityGroupExists(
              indexName,
              adapter.getTypeName())) {
        ((AccumuloOperations) baseOperations).addLocalityGroup(
            indexName,
            adapter.getTypeName(),
            adapter.getAdapterId());
      }
    } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
      LOGGER.error("Unable to determine existence of locality group [" + typeName + "]", e);
    }
  }

  /**
   * This is not a typical resource, it references a static Accumulo connector used by all DataStore
   * instances with common connection parameters. Closing this is only recommended when the JVM no
   * longer needs any connection to this Accumulo store with common connection parameters.
   */
  @Override
  public void close() {
    ((AccumuloOperations) baseOperations).close();
  }

  @Override
  public List<InputSplit> getSplits(
      final CommonQueryOptions commonOptions,
      final DataTypeQueryOptions<?> typeOptions,
      final IndexQueryOptions indexOptions,
      final QueryConstraints constraints,
      final TransientAdapterStore adapterStore,
      final AdapterIndexMappingStore aimStore,
      final DataStatisticsStore statsStore,
      final InternalAdapterStore internalAdapterStore,
      final IndexStore indexStore,
      final JobContext context,
      final Integer minSplits,
      final Integer maxSplits) throws IOException, InterruptedException {
    context.getConfiguration().setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    context.getConfiguration().setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, true);
    return super.getSplits(
        commonOptions,
        typeOptions,
        indexOptions,
        constraints,
        adapterStore,
        aimStore,
        statsStore,
        internalAdapterStore,
        indexStore,
        context,
        minSplits,
        maxSplits);
  }

  @Override
  public void prepareRecordWriter(final Configuration conf) {
    // because accumulo requires a more recent version of guava 22.0, this user
    // classpath must override the default hadoop classpath which has an old
    // version of guava or there will be incompatibility issues
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, true);
  }
}
