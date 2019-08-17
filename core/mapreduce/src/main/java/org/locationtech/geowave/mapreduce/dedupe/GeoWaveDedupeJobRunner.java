/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.dedupe;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.mapreduce.AbstractGeoWaveJobRunner;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * This class can run a basic job to query GeoWave, deduplicating results, and writing the final set
 * of key value pairs to a sequence file. It can be extended for more advanced capabilities or job
 * chaining.
 */
public class GeoWaveDedupeJobRunner extends AbstractGeoWaveJobRunner {

  public GeoWaveDedupeJobRunner(final DataStorePluginOptions dataStoreOptions) {
    super(dataStoreOptions);
  }

  @Override
  protected void configure(final Job job) throws Exception {

    job.setJobName("GeoWave Dedupe (" + dataStoreOptions.getGeoWaveNamespace() + ")");

    job.setMapperClass(GeoWaveDedupeMapper.class);
    job.setCombinerClass(GeoWaveDedupeCombiner.class);
    job.setReducerClass(getReducer());
    job.setMapOutputKeyClass(GeoWaveInputKey.class);
    job.setMapOutputValueClass(ObjectWritable.class);
    job.setOutputKeyClass(GeoWaveInputKey.class);
    job.setOutputValueClass(ObjectWritable.class);

    job.setInputFormatClass(GeoWaveInputFormat.class);
    job.setOutputFormatClass(getOutputFormatClass());
    job.setNumReduceTasks(getNumReduceTasks());

    job.setSpeculativeExecution(false);

    try (final FileSystem fs = FileSystem.get(job.getConfiguration())) {
      final Path outputPath = getHdfsOutputPath();
      fs.delete(outputPath, true);
      FileOutputFormat.setOutputPath(job, outputPath);
    }
  }

  protected String getHdfsOutputBase() {
    return "/tmp";
  }

  @SuppressWarnings("rawtypes")
  protected Class<? extends Reducer> getReducer() {
    return GeoWaveDedupeReducer.class;
  }

  public Path getHdfsOutputPath() {
    return new Path(getHdfsOutputBase() + "/" + dataStoreOptions.getGeoWaveNamespace() + "_dedupe");
  }

  protected Class<? extends OutputFormat> getOutputFormatClass() {
    return SequenceFileOutputFormat.class;
  }

  protected int getNumReduceTasks() {
    return 8;
  }

  public static void main(final String[] args) throws Exception {

    final ConfigOptions opts = new ConfigOptions();
    final MainParameterHolder holder = new MainParameterHolder();

    final OperationParser parser = new OperationParser();
    parser.addAdditionalObject(opts);
    parser.addAdditionalObject(holder);

    // Second round to get everything else.
    final CommandLineOperationParams params = parser.parse(args);

    // Set the datastore plugin
    if (holder.getMainParameter().size() == 0) {
      throw new ParameterException("Must specify datastore name as first argument.");
    }

    // Load the params for config file.
    opts.prepare(params);

    final StoreLoader loader = new StoreLoader(holder.getMainParameter().get(0));
    loader.loadFromConfig((File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT));

    final int res =
        ToolRunner.run(
            new Configuration(),
            new GeoWaveDedupeJobRunner(loader.getDataStorePlugin()),
            args);
    System.exit(res);
  }

  public static class MainParameterHolder {
    @Parameter
    private final List<String> mainParameter = new ArrayList<>();

    public List<String> getMainParameter() {
      return mainParameter;
    }
  }
}
