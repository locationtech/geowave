/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.export;

import java.io.IOException;
import java.util.List;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;

public class VectorMRExportJobRunner extends Configured implements Tool {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorMRExportCommand.class);

  public static final String BATCH_SIZE_KEY = "BATCH_SIZE";
  private final DataStorePluginOptions storeOptions;
  private final VectorMRExportOptions mrOptions;
  private final String hdfsHostPort;
  private final String hdfsPath;

  public VectorMRExportJobRunner(
      final DataStorePluginOptions storeOptions,
      final VectorMRExportOptions mrOptions,
      final String hdfsHostPort,
      final String hdfsPath) {
    this.storeOptions = storeOptions;
    this.mrOptions = mrOptions;
    this.hdfsHostPort = hdfsHostPort;
    this.hdfsPath = hdfsPath;
  }

  /** Main method to execute the MapReduce analytic. */
  public int runJob()
      throws CQLException, IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = super.getConf();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }
    GeoWaveConfiguratorBase.setRemoteInvocationParams(
        hdfsHostPort,
        mrOptions.getResourceManagerHostPort(),
        conf);
    final List<String> typeNames = mrOptions.getTypeNames();
    final PersistentAdapterStore adapterStore = storeOptions.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = storeOptions.createInternalAdapterStore();
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    if ((typeNames != null) && (typeNames.size() > 0)) {
      bldr.setTypeNames(typeNames.toArray(new String[0]));
      // options.setAdapters(Lists.transform(
      // typeNames,
      // new Function<String, DataTypeAdapter<?>>() {
      //
      // @Override
      // public DataTypeAdapter<?> apply(
      // final String input ) {
      // Short internalAdpaterId =
      // internalAdapterStore.getInternalAdapterId(new ByteArrayId(
      // input));
      // return adapterStore.getAdapter(internalAdpaterId);
      // }
      // }));
    }
    conf.setInt(BATCH_SIZE_KEY, mrOptions.getBatchSize());
    final IndexStore indexStore = storeOptions.createIndexStore();
    if (mrOptions.getIndexName() != null) {
      final Index index = indexStore.getIndex(mrOptions.getIndexName());
      if (index == null) {
        JCommander.getConsole().println(
            "Unable to find index '" + mrOptions.getIndexName() + "' in store");
        return -1;
      }
      bldr.indexName(mrOptions.getIndexName());
    }
    if (mrOptions.getCqlFilter() != null) {
      if ((typeNames == null) || (typeNames.size() != 1)) {
        JCommander.getConsole().println("Exactly one type is expected when using CQL filter");
        return -1;
      }
      final String typeName = typeNames.get(0);

      final Short internalAdpaterId = internalAdapterStore.getAdapterId(typeName);
      final InternalDataAdapter<?> adapter =
          storeOptions.createAdapterStore().getAdapter(internalAdpaterId);
      if (adapter == null) {
        JCommander.getConsole().println("Type '" + typeName + "' not found");
        return -1;
      }
      if (!(adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter)) {
        JCommander.getConsole().println("Type '" + typeName + "' does not support vector export");

        return -1;
      }
      bldr.constraints(bldr.constraintsFactory().cqlConstraints(mrOptions.getCqlFilter()));
    }
    GeoWaveInputFormat.setStoreOptions(conf, storeOptions);
    // the above code is a temporary placeholder until this gets merged with
    // the new commandline options
    GeoWaveInputFormat.setQuery(conf, bldr.build(), adapterStore, internalAdapterStore, indexStore);
    final Job job = new Job(conf);

    job.setJarByClass(this.getClass());

    job.setJobName("Exporting to " + hdfsPath);
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputPath(job, new Path(hdfsPath));
    job.setMapperClass(VectorExportMapper.class);
    job.setInputFormatClass(GeoWaveInputFormat.class);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);
    AvroJob.setOutputKeySchema(job, AvroSimpleFeatureCollection.SCHEMA$);
    AvroJob.setMapOutputKeySchema(job, AvroSimpleFeatureCollection.SCHEMA$);

    GeoWaveInputFormat.setMinimumSplitCount(job.getConfiguration(), mrOptions.getMinSplits());
    GeoWaveInputFormat.setMaximumSplitCount(job.getConfiguration(), mrOptions.getMaxSplits());

    boolean retVal = false;
    try {
      retVal = job.waitForCompletion(true);
    } catch (final IOException ex) {
      LOGGER.error("Error waiting for map reduce tile resize job: ", ex);
    }
    return retVal ? 0 : 1;
  }

  public static void main(final String[] args) throws Exception {
    final ConfigOptions opts = new ConfigOptions();
    final OperationParser parser = new OperationParser();
    parser.addAdditionalObject(opts);
    final VectorMRExportCommand command = new VectorMRExportCommand();
    final CommandLineOperationParams params = parser.parse(command, args);
    opts.prepare(params);
    final int res = ToolRunner.run(new Configuration(), command.createRunner(params), args);
    System.exit(res);
  }

  @Override
  public int run(final String[] args) throws Exception {
    return runJob();
  }
}
