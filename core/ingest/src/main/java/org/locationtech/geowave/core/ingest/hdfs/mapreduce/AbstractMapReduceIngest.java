/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.ingest.DataAdapterProvider;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;

/**
 * This class can be sub-classed to run map-reduce jobs within the ingest framework using plugins
 * provided by types that are discovered through SPI.
 *
 * @param <T> The type of map-reduce ingest plugin that can be persisted to the map-reduce job
 *        configuration and used by the mapper and/or reducer to ingest data
 */
public abstract class AbstractMapReduceIngest<T extends Persistable & DataAdapterProvider<?>>
    extends
    Configured implements
    Tool {
  public static final String INGEST_PLUGIN_KEY = "INGEST_PLUGIN";
  public static final String GLOBAL_VISIBILITY_KEY = "GLOBAL_VISIBILITY";
  public static final String INDEX_NAMES_KEY = "INDEX_NAMES";
  private static String JOB_NAME = "%s ingest from %s to namespace %s (%s)";
  protected final DataStorePluginOptions dataStoreOptions;
  protected final List<Index> indices;
  protected final VisibilityOptions ingestOptions;
  protected final Path inputFile;
  protected final String formatPluginName;
  protected final IngestFromHdfsPlugin<?, ?> parentPlugin;
  protected final T ingestPlugin;

  public AbstractMapReduceIngest(
      final DataStorePluginOptions dataStoreOptions,
      final List<Index> indices,
      final VisibilityOptions ingestOptions,
      final Path inputFile,
      final String formatPluginName,
      final IngestFromHdfsPlugin<?, ?> parentPlugin,
      final T ingestPlugin) {
    this.dataStoreOptions = dataStoreOptions;
    this.indices = indices;
    this.ingestOptions = ingestOptions;
    this.inputFile = inputFile;
    this.formatPluginName = formatPluginName;
    this.parentPlugin = parentPlugin;
    this.ingestPlugin = ingestPlugin;
  }

  public String getJobName() {
    return String.format(
        JOB_NAME,
        formatPluginName,
        inputFile.toString(),
        dataStoreOptions.getGeoWaveNamespace(),
        getIngestDescription());
  }

  protected abstract String getIngestDescription();

  protected static String[] getIndexNames(final Configuration conf) {
    final String primaryIndexNamesStr = conf.get(AbstractMapReduceIngest.INDEX_NAMES_KEY);
    if ((primaryIndexNamesStr != null) && !primaryIndexNamesStr.isEmpty()) {
      return primaryIndexNamesStr.split(",");
    }
    return new String[0];
  }

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration conf = getConf();
    conf.set(
        INGEST_PLUGIN_KEY,
        ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(ingestPlugin)));
    if (ingestOptions.getVisibility() != null) {
      conf.set(GLOBAL_VISIBILITY_KEY, ingestOptions.getVisibility());
    }
    final Job job = new Job(conf, getJobName());
    final StringBuilder indexNames = new StringBuilder();
    final List<Index> indexes = new ArrayList<>();
    for (final Index primaryIndex : indices) {
      indexes.add(primaryIndex);
      if (primaryIndex != null) {
        // add index
        GeoWaveOutputFormat.addIndex(job.getConfiguration(), primaryIndex);
        if (indexNames.length() != 0) {
          indexNames.append(",");
        }
        indexNames.append(primaryIndex.getName());
      }
    }

    job.getConfiguration().set(INDEX_NAMES_KEY, indexNames.toString());

    job.setJarByClass(AbstractMapReduceIngest.class);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, parentPlugin.getAvroSchema());
    FileInputFormat.setInputPaths(job, inputFile);

    setupMapper(job);
    setupReducer(job);
    // set geowave output format
    job.setOutputFormatClass(GeoWaveOutputFormat.class);

    GeoWaveOutputFormat.setStoreOptions(job.getConfiguration(), dataStoreOptions);
    final DataStore store = dataStoreOptions.createDataStore();
    final DataTypeAdapter<?>[] dataAdapters =
        ingestPlugin.getDataAdapters(ingestOptions.getVisibility());
    final Index[] indices = indexes.toArray(new Index[indexes.size()]);
    if ((dataAdapters != null) && (dataAdapters.length > 0)) {
      for (final DataTypeAdapter<?> dataAdapter : dataAdapters) {
        // from a controlled client, intialize the writer within the
        // context of the datastore before distributing ingest
        // however, after ingest we should cleanup any pre-created
        // metadata for which there is no data
        store.addType(dataAdapter, indices);

        GeoWaveOutputFormat.addDataAdapter(job.getConfiguration(), dataAdapter);
      }
    } else {
      // if the adapter is unknown by the ingest format, at least add the
      // indices from the client
      for (final Index index : indices) {
        dataStoreOptions.createIndexStore().addIndex(index);
      }
      if (indices.length > 0) {
        for (final MetadataType type : MetadataType.values()) {
          // stats and index metadata writers are created elsewhere
          if (!MetadataType.INDEX.equals(type) && !MetadataType.STATS.equals(type)) {
            dataStoreOptions.createDataStoreOperations().createMetadataWriter(type).close();
          }
        }
      }
    }
    // this is done primarily to ensure stats merging is enabled before the
    // distributed ingest
    if (dataStoreOptions.getFactoryOptions().getStoreOptions().isPersistDataStatistics()) {
      dataStoreOptions.createDataStoreOperations().createMetadataWriter(MetadataType.STATS).close();
    }
    job.setSpeculativeExecution(false);

    // add required indices
    final Index[] requiredIndices = parentPlugin.getRequiredIndices();
    if (requiredIndices != null) {
      for (final Index requiredIndex : requiredIndices) {
        GeoWaveOutputFormat.addIndex(job.getConfiguration(), requiredIndex);
      }
    }
    final int retVal = job.waitForCompletion(true) ? 0 : -1;
    // when it is complete, delete any empty adapters and index mappings
    // that were created from this driver but didn't actually have data
    // ingests
    if ((dataAdapters != null) && (dataAdapters.length > 0)) {
      PersistentAdapterStore adapterStore = null;
      AdapterIndexMappingStore adapterIndexMappingStore = null;
      InternalAdapterStore internalAdapterStore = null;
      for (final DataTypeAdapter<?> dataAdapter : dataAdapters) {
        final String typeName = dataAdapter.getTypeName();
        try (CloseableIterator<?> it =
            store.query(QueryBuilder.newBuilder().addTypeName(typeName).limit(1).build())) {
          if (!it.hasNext()) {
            if (adapterStore == null) {
              adapterStore = dataStoreOptions.createAdapterStore();
              internalAdapterStore = dataStoreOptions.createInternalAdapterStore();
              adapterIndexMappingStore = dataStoreOptions.createAdapterIndexMappingStore();
            }
            final Short adapterId = internalAdapterStore.getAdapterId(typeName);
            if (adapterId != null) {
              internalAdapterStore.remove(adapterId);
              adapterStore.removeAdapter(adapterId);
              adapterIndexMappingStore.remove(adapterId);
            }
          }
        }
      }
    }
    return retVal;
  }

  protected abstract void setupMapper(Job job);

  protected abstract void setupReducer(Job job);
}
