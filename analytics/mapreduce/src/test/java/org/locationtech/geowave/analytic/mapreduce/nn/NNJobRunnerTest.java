/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.nn;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.distance.GeometryCentroidDistanceFn;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.MapReduceIntegration;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters.MRConfig;
import org.locationtech.geowave.analytic.param.PartitionParameters.Partition;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import org.locationtech.geowave.analytic.partitioner.Partitioner;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class NNJobRunnerTest {
  final NNJobRunner jjJobRunner = new NNJobRunner();
  final PropertyManagement runTimeProperties = new PropertyManagement();
  @Rule
  public TestName name = new TestName();

  @Before
  public void init() {
    jjJobRunner.setMapReduceIntegrater(new MapReduceIntegration() {
      @Override
      public int submit(
          final Configuration configuration,
          final PropertyManagement runTimeProperties,
          final GeoWaveAnalyticJobRunner tool) throws Exception {
        tool.setConf(configuration);
        return ToolRunner.run(configuration, tool, new String[] {});
      }

      @Override
      public Counters waitForCompletion(final Job job)
          throws ClassNotFoundException, IOException, InterruptedException {

        Assert.assertEquals(SequenceFileInputFormat.class, job.getInputFormatClass());
        Assert.assertEquals(10, job.getNumReduceTasks());
        final ScopedJobConfiguration configWrapper =
            new ScopedJobConfiguration(job.getConfiguration(), NNMapReduce.class);
        Assert.assertEquals("file://foo/bin", job.getConfiguration().get("mapred.input.dir"));

        Assert.assertEquals(0.4, configWrapper.getDouble(Partition.MAX_DISTANCE, 0.0), 0.001);

        Assert.assertEquals(100, configWrapper.getInt(Partition.MAX_MEMBER_SELECTION, 1));

        try {
          final Partitioner<?> wrapper =
              configWrapper.getInstance(Partition.PARTITIONER_CLASS, Partitioner.class, null);

          Assert.assertEquals(OrthodromicDistancePartitioner.class, wrapper.getClass());

          final Partitioner<?> secondary =
              configWrapper.getInstance(
                  Partition.SECONDARY_PARTITIONER_CLASS,
                  Partitioner.class,
                  null);

          Assert.assertEquals(OrthodromicDistancePartitioner.class, secondary.getClass());

          final DistanceFn<?> distancFn =
              configWrapper.getInstance(
                  CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
                  DistanceFn.class,
                  GeometryCentroidDistanceFn.class);

          Assert.assertEquals(FeatureCentroidDistanceFn.class, distancFn.getClass());

        } catch (final InstantiationException e) {
          throw new IOException("Unable to configure system", e);
        } catch (final IllegalAccessException e) {
          throw new IOException("Unable to configure system", e);
        }

        Assert.assertEquals(10, job.getNumReduceTasks());

        return new Counters();
      }

      @Override
      public Job getJob(final Tool tool) throws IOException {
        return new Job(tool.getConf());
      }

      @Override
      public Configuration getConfiguration(final PropertyManagement runTimeProperties)
          throws IOException {
        return new Configuration();
      }
    });

    jjJobRunner.setInputFormatConfiguration(
        new SequenceFileInputFormatConfiguration(new Path("file://foo/bin")));
    jjJobRunner.setReducerCount(10);

    runTimeProperties.store(MRConfig.HDFS_BASE_DIR, "/");

    DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
        "memory",
        new MemoryStoreFactoryFamily());
    pluginOptions.selectPlugin("memory");
    MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
    final String namespace = "test_" + getClass().getName() + "_" + name.getMethodName();
    opts.setGeoWaveNamespace(namespace);
    PersistableStore store = new PersistableStore(pluginOptions);

    runTimeProperties.store(StoreParam.INPUT_STORE, store);

    runTimeProperties.store(
        CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
        FeatureCentroidDistanceFn.class);

    runTimeProperties.store(Partition.PARTITIONER_CLASS, OrthodromicDistancePartitioner.class);

    runTimeProperties.store(
        Partition.SECONDARY_PARTITIONER_CLASS,
        OrthodromicDistancePartitioner.class);

    runTimeProperties.store(Partition.MAX_DISTANCE, Double.valueOf(0.4));

    runTimeProperties.store(Partition.MAX_MEMBER_SELECTION, Integer.valueOf(100));
  }

  @Test
  public void test() throws Exception {

    jjJobRunner.run(runTimeProperties);
  }
}
